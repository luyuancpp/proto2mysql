package pbconv

import (
	"encoding/base64"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strconv"
	"time"
)

var (
	t                 = timestamppb.Timestamp{}
	timestampFullName = t.ProtoReflect().Descriptor().FullName()

	ErrInvalidFieldKind = errors.New("invalid field kind")
)

// MapWrapper 临时包装器：用于序列化map
type MapWrapper struct {
	MapData protoreflect.Map
}

func (m *MapWrapper) ProtoReflect() protoreflect.Message {
	return protoreflect.Message(nil)
}

// ListWrapper 临时包装器：用于序列化list
type ListWrapper struct {
	ListData protoreflect.List
}

func (l *ListWrapper) ProtoReflect() protoreflect.Message {
	return protoreflect.Message(nil)
}

// 序列化字段（完善二进制编码和对称性）
func SerializeFieldAsString(message proto.Message, fieldDesc protoreflect.FieldDescriptor) (string, error) {
	reflection := message.ProtoReflect()

	// 特殊处理 Timestamp 类型
	if fieldDesc.Kind() == protoreflect.MessageKind && fieldDesc.Message() != nil && fieldDesc.Message().FullName() == timestampFullName {
		if !reflection.Has(fieldDesc) {
			return "", nil
		}
		subMsg := reflection.Get(fieldDesc).Message().Interface()
		ts, ok := subMsg.(*timestamppb.Timestamp)
		if !ok {
			return "", fmt.Errorf("field %s is not a Timestamp", fieldDesc.Name())
		}
		if ts.AsTime().IsZero() {
			return "", nil
		}
		return ts.AsTime().Format("2006-01-02 15:04:05"), nil
	}

	// 处理 map 类型（Base64 编码）
	if fieldDesc.IsMap() {
		if !reflection.Has(fieldDesc) {
			return "", nil
		}
		mapWrapper := &MapWrapper{MapData: reflection.Get(fieldDesc).Map()}
		data, err := proto.Marshal(mapWrapper)
		if err != nil {
			return "", fmt.Errorf("serialize map field %s: %w", fieldDesc.Name(), err)
		}
		return base64.StdEncoding.EncodeToString(data), nil
	}

	// 处理 list 类型（Base64 编码）
	if fieldDesc.IsList() {
		if !reflection.Has(fieldDesc) {
			return "", nil
		}
		listWrapper := &ListWrapper{ListData: reflection.Get(fieldDesc).List()}
		data, err := proto.Marshal(listWrapper)
		if err != nil {
			return "", fmt.Errorf("serialize list field %s: %w", fieldDesc.Name(), err)
		}
		return base64.StdEncoding.EncodeToString(data), nil
	}

	// 处理基本类型
	switch fieldDesc.Kind() {
	case protoreflect.Int32Kind:
		return fmt.Sprintf("%d", reflection.Get(fieldDesc).Int()), nil
	case protoreflect.Uint32Kind:
		return fmt.Sprintf("%d", reflection.Get(fieldDesc).Uint()), nil
	case protoreflect.FloatKind:
		val := reflection.Get(fieldDesc).Float()
		return strconv.FormatFloat(val, 'f', -1, 32), nil
	case protoreflect.StringKind:
		return reflection.Get(fieldDesc).String(), nil
	case protoreflect.Int64Kind:
		return strconv.FormatInt(reflection.Get(fieldDesc).Int(), 10), nil
	case protoreflect.Uint64Kind:
		return strconv.FormatUint(reflection.Get(fieldDesc).Uint(), 10), nil
	case protoreflect.DoubleKind:
		val := reflection.Get(fieldDesc).Float()
		return strconv.FormatFloat(val, 'f', -1, 64), nil
	case protoreflect.BoolKind:
		return fmt.Sprintf("%t", reflection.Get(fieldDesc).Bool()), nil
	case protoreflect.EnumKind:
		return fmt.Sprintf("%d", int32(reflection.Get(fieldDesc).Enum())), nil
	case protoreflect.BytesKind:
		return base64.StdEncoding.EncodeToString(reflection.Get(fieldDesc).Bytes()), nil
	case protoreflect.MessageKind:
		if reflection.Has(fieldDesc) {
			subMsg := reflection.Get(fieldDesc).Message().Interface()
			data, err := proto.Marshal(subMsg)
			if err != nil {
				return "", fmt.Errorf("marshal sub-message field %s: %w", fieldDesc.Name(), err)
			}
			return base64.StdEncoding.EncodeToString(data), nil
		}
		return "", nil
	default:
		return "", fmt.Errorf("%w: %v (field: %s)", ErrInvalidFieldKind, fieldDesc.Kind(), fieldDesc.Name())
	}
}

// 反序列化字段（完善二进制解码和时间格式容错）
func ParseFromString(message proto.Message, row []string) error {
	reflection := message.ProtoReflect()
	desc := reflection.Descriptor()

	for i := 0; i < desc.Fields().Len(); i++ {
		if i >= len(row) {
			continue
		}

		fieldDesc := desc.Fields().Get(i)
		fieldValue := row[i]
		fieldName := fieldDesc.Name()

		// 特殊处理 Timestamp 类型（支持多格式）
		if fieldDesc.Message() != nil && fieldDesc.Message().FullName() == timestampFullName {
			if fieldValue == "" {
				continue
			}
			formats := []string{
				"2006-01-02 15:04:05.999", // 带毫秒
				"2006-01-02 15:04:05",     // 不带毫秒
				"2006-01-02",              // 仅日期
			}
			var t time.Time
			var err error
			for _, format := range formats {
				t, err = time.Parse(format, fieldValue)
				if err == nil {
					break
				}
			}
			if err != nil {
				return fmt.Errorf("parse timestamp field %s: %w (value: %s)", fieldName, err, fieldValue)
			}
			ts := timestamppb.New(t)
			data, err := proto.Marshal(ts)
			if err != nil {
				return fmt.Errorf("marshal timestamp field %s: %w", fieldName, err)
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfBytes(data))
			continue
		}

		// 处理 map 类型（Base64 解码）
		if fieldDesc.IsMap() {
			if fieldValue == "" {
				continue
			}
			data, err := base64.StdEncoding.DecodeString(fieldValue)
			if err != nil {
				return fmt.Errorf("decode map field %s: %w (value: %s)", fieldName, err, fieldValue)
			}
			mapWrapper := &MapWrapper{}
			if err := proto.Unmarshal(data, mapWrapper); err != nil {
				return fmt.Errorf("parse map field %s: %w (value: %s)", fieldName, err, fieldValue)
			}
			mapVal := reflection.Mutable(fieldDesc).Map()
			mapWrapper.MapData.Range(func(key protoreflect.MapKey, val protoreflect.Value) bool {
				mapVal.Set(key, val)
				return true
			})
			continue
		}

		// 处理 list 类型（Base64 解码）
		if fieldDesc.IsList() {
			if fieldValue == "" {
				continue
			}
			data, err := base64.StdEncoding.DecodeString(fieldValue)
			if err != nil {
				return fmt.Errorf("decode list field %s: %w (value: %s)", fieldName, err, fieldValue)
			}
			listWrapper := &ListWrapper{}
			if err := proto.Unmarshal(data, listWrapper); err != nil {
				return fmt.Errorf("parse list field %s: %w (value: %s)", fieldName, err, fieldValue)
			}
			listVal := reflection.Mutable(fieldDesc).List()
			for i := 0; i < listWrapper.ListData.Len(); i++ {
				listVal.Append(listWrapper.ListData.Get(i))
			}
			continue
		}

		// 非集合类型处理
		if fieldValue == "" {
			switch fieldDesc.Kind() {
			case protoreflect.Int32Kind, protoreflect.Int64Kind:
				reflection.Set(fieldDesc, protoreflect.ValueOfInt64(0))
			case protoreflect.Uint32Kind, protoreflect.Uint64Kind:
				reflection.Set(fieldDesc, protoreflect.ValueOfUint64(0))
			case protoreflect.FloatKind, protoreflect.DoubleKind:
				reflection.Set(fieldDesc, protoreflect.ValueOfFloat64(0))
			case protoreflect.BoolKind:
				reflection.Set(fieldDesc, protoreflect.ValueOfBool(false))
			case protoreflect.StringKind:
				reflection.Set(fieldDesc, protoreflect.ValueOfString(""))
			}
			continue
		}

		// 解析非空值
		switch fieldDesc.Kind() {
		case protoreflect.Int32Kind:
			val, err := strconv.ParseInt(row[i], 10, 32)
			if err != nil {
				return fmt.Errorf("parse int32 field %s: %w (value: %s)", fieldName, err, row[i])
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfInt32(int32(val)))
		case protoreflect.Int64Kind:
			val, err := strconv.ParseInt(row[i], 10, 64)
			if err != nil {
				return fmt.Errorf("parse int64 field %s: %w (value: %s)", fieldName, err, row[i])
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfInt64(val))
		case protoreflect.Uint32Kind:
			val, err := strconv.ParseUint(row[i], 10, 32)
			if err != nil {
				return fmt.Errorf("parse uint32 field %s: %w (value: %s)", fieldName, err, row[i])
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfUint32(uint32(val)))
		case protoreflect.Uint64Kind:
			val, err := strconv.ParseUint(row[i], 10, 64)
			if err != nil {
				return fmt.Errorf("parse uint64 field %s: %w (value: %s)", fieldName, err, row[i])
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfUint64(val))
		case protoreflect.FloatKind:
			val, err := strconv.ParseFloat(row[i], 32)
			if err != nil {
				return fmt.Errorf("parse float field %s: %w (value: %s)", fieldName, err, row[i])
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfFloat32(float32(val)))
		case protoreflect.DoubleKind:
			val, err := strconv.ParseFloat(row[i], 64)
			if err != nil {
				return fmt.Errorf("parse double field %s: %w (value: %s)", fieldName, err, row[i])
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfFloat64(val))
		case protoreflect.StringKind:
			reflection.Set(fieldDesc, protoreflect.ValueOfString(row[i]))
		case protoreflect.BoolKind:
			val, err := strconv.ParseBool(row[i])
			if err != nil {
				return fmt.Errorf("parse bool field %s: %w (value: %s)", fieldName, err, row[i])
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfBool(val))
		case protoreflect.EnumKind:
			val, err := strconv.Atoi(row[i])
			if err != nil {
				return fmt.Errorf("parse enum field %s: %w (value: %s)", fieldName, err, row[i])
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfEnum(protoreflect.EnumNumber(val)))
		case protoreflect.BytesKind:
			data, err := base64.StdEncoding.DecodeString(row[i])
			if err != nil {
				return fmt.Errorf("decode bytes field %s: %w", fieldName, err)
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfBytes(data))
		case protoreflect.MessageKind:
			data, err := base64.StdEncoding.DecodeString(row[i])
			if err != nil {
				return fmt.Errorf("decode sub-message field %s: %w", fieldName, err)
			}
			subMsg := reflection.Mutable(fieldDesc).Message()
			if err := proto.Unmarshal(data, subMsg.Interface()); err != nil {
				return fmt.Errorf("unmarshal sub-message field %s: %w (value: %s)", fieldName, err, row[i])
			}
		default:
			return fmt.Errorf("%w: %v (field: %s)", ErrInvalidFieldKind, fieldDesc.Kind(), fieldName)
		}
	}

	return nil
}
