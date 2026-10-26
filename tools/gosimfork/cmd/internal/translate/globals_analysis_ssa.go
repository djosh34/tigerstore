package translate

import (
	"go/ast"
	"go/token"
	"go/types"
	"log"
	"strings"

	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

func collectFuncs(pkg *packages.Package, ssaPkg *ssa.Package) []*ssa.Function {
	// Compute list of source functions, including literals,
	// in source order.
	var funcs []*ssa.Function
	for _, f := range pkg.Syntax {
		for _, decl := range f.Decls {
			if fdecl, ok := decl.(*ast.FuncDecl); ok {
				// (init functions have distinct Func
				// objects named "init" and distinct
				// ssa.Functions named "init#1", ...)

				fn := pkg.TypesInfo.Defs[fdecl.Name].(*types.Func)
				if fn == nil {
					panic(fn)
				}

				f := ssaPkg.Prog.FuncValue(fn)
				if f == nil {
					panic(fn)
				}

				var addAnons func(f *ssa.Function)
				addAnons = func(f *ssa.Function) {
					funcs = append(funcs, f)
					for _, anon := range f.AnonFuncs {
						addAnons(anon)
					}
				}
				addAnons(f)
			}
		}
	}
	// XXX: inits? other crap?
	return funcs
}

type mapInitializer struct {
	name     string
	onceName string
	onceFn   string
}

type ssaGlobalsInfo struct {
	readonlyMaps []mapInitializer
}

func collectGlobalsSSA(pkg *packages.Package) ssaGlobalsInfo {
	program, pkgs := ssautil.Packages([]*packages.Package{pkg}, ssa.BuilderMode(0))
	_ = program

	// log.Println(program, pkgs)

	if len(pkgs) != 1 {
		log.Fatal("wtf")
	}

	// XXX: should we check what kind of object is in the map? what if is mutable? (i mean it shouldn't be but........)

	maps := make(map[types.Object]struct{})
	mapInstrs := make(map[types.Object][]ssa.Instruction)

	funRefs := make(map[types.Object][]ssa.Instruction)

	ssaPkg := pkgs[0]
	ssaPkg.Build()

	for _, member := range ssaPkg.Members {
		switch member := member.(type) {
		case *ssa.Global:
			// log.Println(member.Name(), member.Type())
			_, ok := member.Type().(*types.Pointer).Elem().(*types.Map)
			if ok {
				// log.Println(member)
				maps[member.Object()] = struct{}{}
			}
		}
	}

	var rands []*ssa.Value

	for _, fun := range collectFuncs(pkg, ssaPkg) {
		for _, block := range fun.Blocks {
			for _, instr := range block.Instrs {
				rands = instr.Operands(rands[:0])
				for _, op := range rands {
					if glob, ok := (*op).(*ssa.Global); ok {
						if glob.Pkg == ssaPkg {
							if _, ok := maps[glob.Object()]; ok {
								mapInstrs[glob.Object()] = append(mapInstrs[glob.Object()], instr)
							}
						}
					}
					if fun, ok := (*op).(*ssa.Function); ok {
						if fun.Object() != nil {
							funRefs[fun.Object()] = append(funRefs[fun.Object()], instr)
						}
					}
				}
			}
		}
	}

	var globals ssaGlobalsInfo

	for obj, instrs := range mapInstrs {
		// log.Println(obj)
		readonly := true
		weird := false
		maybeInitializers := make(map[*ssa.Function]struct{})

		for _, instr := range instrs {
			// instr.Block().Parent().WriteTo(os.Stdout)
			// log.Println(instr)

			switch instr := instr.(type) {
			case ssa.Value:
				// log.Println("read", instr.Name(), instr.String())
				for _, next := range *instr.Referrers() {
					switch next := next.(type) {
					case *ssa.Lookup:
						// log.Println("indexed, ok")
					case *ssa.MapUpdate:
						// log.Println("updated, not ok")
						maybeInitializers[instr.Parent()] = struct{}{}
					default:
						readonly = false
						weird = true
						_ = next
						// log.Println("unknown", next)
					}
				}
			case *ssa.Store:
				maybeInitializers[instr.Parent()] = struct{}{}
				// log.Println("write", instr.String(), instr.Val)
			default:
				readonly = false
				weird = true
				// log.Println("wtf", reflect.TypeOf(instr))
			}
		}

		if weird {
			// log.Println("weird, can't do it")
		} else if readonly && len(maybeInitializers) == 0 {
			// log.Println("yay, readonly")
			globals.readonlyMaps = append(globals.readonlyMaps, mapInitializer{
				name: obj.Name(),
			})
		} else if readonly && len(maybeInitializers) > 0 {
			// log.Println(maybeInitializers)
			if len(maybeInitializers) != 1 {
				// log.Println("more than one initializer, bailing...")
			} else {
				var fun *ssa.Function
				for f := range maybeInitializers {
					fun = f
				}
				// log.Println("checking fun", fun.Object())

				onceName := ""
				thisFunGood := fun.Parent() == nil // xxx can't handle nested funs
				thisFunGood = thisFunGood && !token.IsExported(fun.Name()) && !strings.HasPrefix(fun.Name(), "init#")
				for _, ref := range funRefs[fun.Object()] {
					thisRefGood := false

					// log.Println("checking ref", ref)
					if call, ok := ref.(*ssa.Call); ok {
						if fun, ok := call.Call.Value.(*ssa.Function); ok {
							if obj, ok := fun.Object().(*types.Func); ok && obj.FullName() == "(*sync.Once).Do" { // can this be faster please??
								// XXX: GREAT only initialized once. now double-check that this once doesn't do any other funny business!!!
								// ie its global, and if any other calls to Do also exist for this function they must use the same once
								// (i mean why not but let's just check...)
								// log.Println("seems good!!")
								// log.Println(call)

								onceVar := call.Call.Args[0]
								// log.Println(onceVar, reflect.TypeOf(onceVar))
								if onceGlobal, ok := onceVar.(*ssa.Global); ok {
									if onceName == "" {
										onceName = onceGlobal.Name()
										thisRefGood = true
									} else {
										thisRefGood = onceName == onceGlobal.Name()
									}
								}

								// XXX: now check that this once is only used for this func...???
							}
						}
					}

					thisFunGood = thisFunGood && thisRefGood
				}
				if thisFunGood {
					// log.Println("we declare it readonly", onceName)
					globals.readonlyMaps = append(globals.readonlyMaps, mapInitializer{
						name:     obj.Name(),
						onceName: onceName,
						onceFn:   fun.Name(),
					})
				}
			}
		}

		// log.Println()
		// inits: all call from ssaPkg.Members["init"] to init-named funcs
	}

	// next:
	// - grpc/protobuf? pretty close and would be a big win... then afterwards clean this up.
	// - arrays? slices?
	// - only for primitives for now? or do same sloppiness as with maps? (will get caught by shared sync.Pool...)

	// XXX: public maps bad
	// XXX: registeredCodecs in google.golang.org/grpc/encoding???
	// XXX: init for go/token keywords???

	return globals

	// XXX: can we get the init functions and initializers in the house also please?
	// XXX: want to make sure they don't deref or anything

	// XXX: maybe a different helpful analysis is figuring out if a field can be
	// accessed without holding a lock (or some other sync thing?) anything
	// that you can get to we can mostly conclude to be immutable...???

	/*
		for _, member := range ssaPkg.Members {
			switch member := member.(type) {
			case *ssa.Function:
				if token.IsExported(member.Name()) {
					log.Println(member.Name(), member.Params)
				}

			case *ssa.Type:
				if token.IsExported(member.Name()) {
					log.Println(member.Name())
					methods := program.MethodSets.MethodSet(member.Type())
					for i := 0; i < methods.Len(); i++ {
						log.Println(methods.At(i))
					}
				}
			}
		}
	*/
}
