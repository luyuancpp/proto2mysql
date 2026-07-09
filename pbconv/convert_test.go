package pbconv

import (
	"testing"

	messageoption "github.com/luyuancpp/protooption"
	"google.golang.org/protobuf/proto"
)

// TestScalarFieldRoundTrip 验证标量与嵌套消息字段的序列化/反序列化对称性
func TestScalarFieldRoundTrip(t *testing.T) {
	src := &messageoption.GolangTest{
		Id:      42,
		GroupId: 7,
		Ip:      "127.0.0.1",
		Port:    3306,
		Player: &messageoption.Player{
			PlayerId: 100,
			Name:     "foo'bar\\baz\n中文😊",
		},
	}

	desc := src.ProtoReflect().Descriptor()
	row := make([]string, desc.Fields().Len())
	for i := 0; i < desc.Fields().Len(); i++ {
		val, err := SerializeFieldAsString(src, desc.Fields().Get(i))
		if err != nil {
			t.Fatalf("serialize field %s: %v", desc.Fields().Get(i).Name(), err)
		}
		row[i] = val
	}

	dst := &messageoption.GolangTest{}
	if err := ParseFromString(dst, row); err != nil {
		t.Fatalf("parse row: %v", err)
	}

	if !proto.Equal(src, dst) {
		t.Errorf("round trip mismatch\nwant: %s\ngot:  %s", src.String(), dst.String())
	}
}

// TestRepeatedFieldRoundTrip 验证repeated字段（旧实现会panic）的序列化/反序列化
func TestRepeatedFieldRoundTrip(t *testing.T) {
	src := &messageoption.GolangTestList{
		TestList: []*messageoption.GolangTest{
			{Id: 1, Ip: "10.0.0.1", Port: 1},
			{Id: 2, Ip: "10.0.0.2", Port: 2, Player: &messageoption.Player{PlayerId: 9, Name: "p"}},
		},
	}

	fieldDesc := src.ProtoReflect().Descriptor().Fields().Get(0)
	if !fieldDesc.IsList() {
		t.Fatalf("expected first field of GolangTestList to be repeated")
	}

	encoded, err := SerializeFieldAsString(src, fieldDesc)
	if err != nil {
		t.Fatalf("serialize repeated field: %v", err)
	}
	if encoded == "" {
		t.Fatal("expected non-empty encoded value")
	}

	dst := &messageoption.GolangTestList{}
	if err := ParseFromString(dst, []string{encoded}); err != nil {
		t.Fatalf("parse repeated field: %v", err)
	}

	if !proto.Equal(src, dst) {
		t.Errorf("round trip mismatch\nwant: %s\ngot:  %s", src.String(), dst.String())
	}
}

// TestEmptyValuesRoundTrip 验证空值/未设置字段的处理
func TestEmptyValuesRoundTrip(t *testing.T) {
	src := &messageoption.GolangTest{}

	desc := src.ProtoReflect().Descriptor()
	row := make([]string, desc.Fields().Len())
	for i := 0; i < desc.Fields().Len(); i++ {
		val, err := SerializeFieldAsString(src, desc.Fields().Get(i))
		if err != nil {
			t.Fatalf("serialize field %s: %v", desc.Fields().Get(i).Name(), err)
		}
		row[i] = val
	}

	dst := &messageoption.GolangTest{}
	if err := ParseFromString(dst, row); err != nil {
		t.Fatalf("parse row: %v", err)
	}

	if !proto.Equal(src, dst) {
		t.Errorf("round trip mismatch\nwant: %s\ngot:  %s", src.String(), dst.String())
	}
}
