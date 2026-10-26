package reflect

import "reflect" //gosim:notranslate

func Swapper(slice any) func(i, j int) {
	return reflect.Swapper(slice)
}
