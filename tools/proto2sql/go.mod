module github.com/luyuancpp/proto2sql

go 1.26.5

require (
	github.com/bufbuild/protocompile v0.14.1
	github.com/luyuancpp/proto2mysql v0.0.0-00010101000000-000000000000
	google.golang.org/protobuf v1.36.10
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/go-sql-driver/mysql v1.8.1 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	golang.org/x/sync v0.9.0 // indirect
	golang.org/x/text v0.20.0 // indirect
	gorm.io/gorm v1.30.0 // indirect
)

// 本地开发：指向仓库根的 proto2mysql（复用其 SQL 生成内核）。
// 若拆分为独立仓库，改为正式版本号即可。
replace github.com/luyuancpp/proto2mysql => ../../
