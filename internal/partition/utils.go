package partition

// TODO: Rewrite this bullshit, this is a potential bug,
// if two different partition ask for a next number, both of them can receive the same,
// which will lead to the full fucking up of the whole log file and its segments.
// freeNext returns a free next number available
// func freeNext(path string, ext string) (int64, error) {
// 	var biggestNumber int64 = 0
//
// 	dir, err := os.ReadDir(path)
// 	if err != nil {
// 		return 0, err
// 	}
//
// 	for _, file := range dir {
// 		info, err := file.Info()
// 		if err != nil {
// 			return 0, err
// 		}
//
// 		var extension string
// 		var number int64
//
// 		if _, err := fmt.Sscanf(info.Name(), "%08d.%s", &number, &extension); err == nil {
// 			if biggestNumber < number && extension == ext {
// 				biggestNumber = number
// 			}
// 		}
// 	}
//
// 	return biggestNumber + 1, nil
// }
