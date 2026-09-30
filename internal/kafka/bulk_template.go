package kafka

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/google/uuid"
)

// 模板占位符说明（语义接近 Mock.js 常用类型，但不是完整 Mock.js 语法引擎）：
//
//	基础：{{seq}} {{seq1}} {{uuid}} {{timestamp}} {{now}} {{rand}}
//	人物：{{name}} {{firstName}} {{lastName}} {{cname}} {{age}}
//	联系：{{email}} {{phone}} {{mobile}}
//	地址：{{address}} {{caddress}} {{city}} {{ccity}} {{company}}
//	其它：{{ip}} {{url}} {{bool}} {{text}}
//	参数：{{integer:min:max}} {{string:n}} {{text:n}} {{pick:a|b|c}}

var (
	reInteger = regexp.MustCompile(`\{\{integer:(-?\d+):(-?\d+)\}\}`)
	reString  = regexp.MustCompile(`\{\{string:(\d+)\}\}`)
	reText    = regexp.MustCompile(`\{\{text:(\d+)\}\}`)
	rePick    = regexp.MustCompile(`\{\{pick:([^}]+)\}\}`)
	reSimple  = regexp.MustCompile(`\{\{(seq|seq1|uuid|timestamp|now|rand|name|firstName|lastName|cname|age|email|phone|mobile|address|caddress|city|ccity|company|ip|url|bool|boolean|text)\}\}`)
)

var cnSurnames = []string{
	"王", "李", "张", "刘", "陈", "杨", "黄", "赵", "周", "吴",
	"徐", "孙", "马", "朱", "胡", "郭", "何", "林", "高", "罗",
}

var cnGiven = []string{
	"伟", "芳", "娜", "敏", "静", "丽", "强", "磊", "军", "洋",
	"勇", "艳", "杰", "涛", "超", "秀英", "桂英", "晓明", "建华", "志强",
	"海燕", "子轩", "雨桐", "一诺", "思远", "浩然", "欣怡", "雅婷", "俊杰", "文博",
}

var cnCities = []string{
	"北京", "上海", "广州", "深圳", "杭州", "南京", "成都", "武汉", "西安", "重庆",
	"苏州", "天津", "长沙", "郑州", "青岛", "大连", "厦门", "宁波", "合肥", "福州",
}

var cnDistricts = []string{
	"朝阳区", "海淀区", "浦东新区", "天河区", "南山新区", "西湖区", "锦江区", "江汉区", "雁塔区", "渝中区",
}

var cnStreets = []string{
	"中山路", "人民路", "解放路", "建设路", "和平路", "文化路", "科技大道", "滨江路", "学院路", "工业园路",
}

// renderBulkValue 按序号渲染消息模板。
func renderBulkValue(tpl string, seq int) []byte {
	now := time.Now()
	out := tpl

	out = reInteger.ReplaceAllStringFunc(out, func(m string) string {
		sub := reInteger.FindStringSubmatch(m)
		if len(sub) != 3 {
			return m
		}
		min, _ := strconv.Atoi(sub[1])
		max, _ := strconv.Atoi(sub[2])
		if max < min {
			min, max = max, min
		}
		return strconv.Itoa(gofakeit.Number(min, max))
	})
	out = reString.ReplaceAllStringFunc(out, func(m string) string {
		sub := reString.FindStringSubmatch(m)
		if len(sub) != 2 {
			return m
		}
		n, _ := strconv.Atoi(sub[1])
		if n <= 0 {
			n = 8
		}
		if n > 256 {
			n = 256
		}
		return gofakeit.LetterN(uint(n))
	})
	out = reText.ReplaceAllStringFunc(out, func(m string) string {
		sub := reText.FindStringSubmatch(m)
		if len(sub) != 2 {
			return m
		}
		n, _ := strconv.Atoi(sub[1])
		return randomText(n)
	})
	out = rePick.ReplaceAllStringFunc(out, func(m string) string {
		sub := rePick.FindStringSubmatch(m)
		if len(sub) != 2 {
			return m
		}
		opts := strings.Split(sub[1], "|")
		if len(opts) == 0 {
			return ""
		}
		return opts[rand.IntN(len(opts))]
	})

	// 同一条内多次出现同名占位符时各自独立取值
	out = reSimple.ReplaceAllStringFunc(out, func(m string) string {
		tag := strings.TrimSuffix(strings.TrimPrefix(m, "{{"), "}}")
		return freshPlaceholder(tag, seq, now)
	})
	return []byte(out)
}

func freshPlaceholder(tag string, seq int, now time.Time) string {
	switch tag {
	case "seq":
		return strconv.Itoa(seq)
	case "seq1":
		return strconv.Itoa(seq + 1)
	case "uuid":
		return uuid.NewString()
	case "timestamp":
		return strconv.FormatInt(now.UnixMilli(), 10)
	case "now":
		return time.Now().Format(time.RFC3339Nano)
	case "rand":
		return strconv.Itoa(rand.IntN(1_000_000))
	case "name":
		return gofakeit.Name()
	case "firstName":
		return gofakeit.FirstName()
	case "lastName":
		return gofakeit.LastName()
	case "cname":
		return cnName()
	case "age":
		return strconv.Itoa(gofakeit.Number(18, 65))
	case "email":
		return gofakeit.Email()
	case "phone":
		return gofakeit.Phone()
	case "mobile":
		return cnMobile()
	case "address":
		return gofakeit.Address().Address
	case "caddress":
		return cnAddress()
	case "city":
		return gofakeit.City()
	case "ccity":
		return cnCities[rand.IntN(len(cnCities))]
	case "company":
		return gofakeit.Company()
	case "ip":
		return gofakeit.IPv4Address()
	case "url":
		return gofakeit.URL()
	case "bool", "boolean":
		return strconv.FormatBool(gofakeit.Bool())
	case "text":
		return randomText(48)
	default:
		return "{{" + tag + "}}"
	}
}

func randomText(n int) string {
	if n <= 0 {
		n = 32
	}
	if n > 4096 {
		n = 4096
	}
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	var b strings.Builder
	b.Grow(n)
	for i := 0; i < n; i++ {
		if i > 0 && i%6 == 0 {
			b.WriteByte(' ')
			continue
		}
		b.WriteByte(letters[rand.IntN(len(letters))])
	}
	return b.String()
}

// randomTextBytes 生成恰好 n 字节的可打印 ASCII 文本（用于按字节补齐）。
func randomTextBytes(n int) []byte {
	if n <= 0 {
		return nil
	}
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ "
	out := make([]byte, n)
	for i := 0; i < n; i++ {
		out[i] = letters[rand.IntN(len(letters))]
	}
	return out
}

// padBulkValueToSize 若目标大小 > 当前体积，用随机文本补齐。
// 对 JSON 对象优先写入 "_text" 字段以尽量保持合法 JSON；否则直接追加字节。
func padBulkValueToSize(payload []byte, size int) []byte {
	if size <= 0 || len(payload) >= size {
		return payload
	}
	need := size - len(payload)
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) >= 2 && trimmed[0] == '{' && trimmed[len(trimmed)-1] == '}' {
		// `,"_text":"` + content + `"`  => 11 + len(content)
		const overhead = 11
		if need > overhead {
			fill := randomTextBytes(need - overhead)
			out := make([]byte, 0, size)
			out = append(out, trimmed[:len(trimmed)-1]...)
			out = append(out, []byte(`,"_text":"`)...)
			out = append(out, fill...)
			out = append(out, '"', '}')
			if len(out) == size {
				return out
			}
			// 极端情况下再兜底追加/截断到精确字节
			if len(out) < size {
				return append(out, randomTextBytes(size-len(out))...)
			}
			return out[:size]
		}
	}
	return append(payload, randomTextBytes(need)...)
}

func cnName() string {
	return cnSurnames[rand.IntN(len(cnSurnames))] + cnGiven[rand.IntN(len(cnGiven))]
}

func cnMobile() string {
	prefix := []string{"130", "131", "132", "133", "135", "136", "137", "138", "139", "150", "151", "152", "155", "156", "157", "158", "159", "186", "187", "188", "189"}
	return fmt.Sprintf("%s%08d", prefix[rand.IntN(len(prefix))], rand.IntN(100_000_000))
}

func cnAddress() string {
	return fmt.Sprintf("%s%s%s%d号",
		cnCities[rand.IntN(len(cnCities))],
		cnDistricts[rand.IntN(len(cnDistricts))],
		cnStreets[rand.IntN(len(cnStreets))],
		gofakeit.Number(1, 999),
	)
}
