package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/xujiajun/nutsdb"
)

func ScanByKey(db *nutsdb.DB, bucket string, prefixStr string, offsetNum int, limitNum int) {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			prefix := []byte(prefixStr)

			// 从offset=0开始 ，限制 100 entries 返回
			if entries, _, err := tx.PrefixScan(bucket, prefix, offsetNum, limitNum); err != nil {
				return err
			} else {
				for _, entry := range entries {
					fmt.Println(string(entry.Key), string(entry.Value))
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func IterateBuckets(db *nutsdb.DB) []string {
	bucketList := []string{}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			return tx.IterateBuckets(nutsdb.DataStructureBPTree, func(bucket string) {
				bucketList = append(bucketList, bucket)
			})
		}); err != nil {
		log.Fatal(err)
	}

	return bucketList
}

func GetAllKey(db *nutsdb.DB, bucket string) {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			entries, err := tx.GetAll(bucket)
			if err != nil {
				return err
			}

			for _, entry := range entries {
				fmt.Println("【KEY】: ", string(entry.Key), "\nvalue: ", string(entry.Value))
				fmt.Println("Size: ", entry.Size())
				fmt.Println("TTL: ", entry.Meta.TTL, " Timestamp: ", entry.Meta.Timestamp)
			}

			return nil
		}); err != nil {
		log.Println(err)
	}
}

func main() {
	var dbpath = flag.String("dbpath", "dbpath", "Input your dbpath")
	flag.Parse()

	opt := nutsdb.DefaultOptions
	opt.Dir = *dbpath
	db, err := nutsdb.Open(opt)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	bucketList := IterateBuckets(db)

	for i := 0; i < len(bucketList); i++ {
		fmt.Println("\nbucket: ", bucketList[i])
		GetAllKey(db, bucketList[i])
	}
}
