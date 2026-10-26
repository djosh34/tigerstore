package behavior

func PrivateGlobalPtr() *int {
	return &privateGlobal
}

func init() {
	CopiedPrivateGlobal = privateGlobal
}

var InitializedPrivateGlobal = 35

var CopiedPrivateGlobal int
