package template

import (
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v4"

	"github.com/xitonix/trubka/internal"
)

const (
	floatEx            = `[-+]?[0-9]*\.?[0-9]+`
	intEx              = `[0-9]+`
	signedIntEx        = `[-+]?[0-9]+`
	intPlaceHolderEx   = "([0-9]|#)*"
	floatPlaceHolderEx = `[-+]?([0-9]|#)*\.?([0-9]|#)+`
)

// Parser represents a type to parse random data generator functions within a template.
type Parser struct {
	fixedOffsetEx *regexp.Regexp
}

// NewParser creates a new template parser.
func NewParser() *Parser {
	return &Parser{
		fixedOffsetEx: regexp.MustCompile(`(?i)UTC[+-][0-9]+(:[0-9]+)?`),
	}
}

// Parse parses the input template and replaces random data generator functions with values.
func (t *Parser) Parse(value string) (string, error) {
	gofakeit.Seed(time.Now().UnixNano())
	var err error
	// Ranges need to be processed before normal numbers
	value, err = t.replaceIntRangeGenerators(value)
	if err != nil {
		return "", err
	}
	value, err = t.replaceFloatRangeGenerators(value)
	if err != nil {
		return "", err
	}

	value = t.replaceIntNumberGenerators(value)
	value = t.replaceFloatNumberGenerators(value)
	value = t.replaceExtraGenerators(value)
	value, err = t.replaceTimestampGenerators(value)
	if err != nil {
		return "", err
	}
	value = t.replaceB64Generators(value)
	return value, nil
}

func (t *Parser) replaceFloatRangeGenerators(value string) (result string, err error) {
	// Float range: F[from:to:<optional decimal places>]
	floatRangeEx := regexp.MustCompile(
		fmt.Sprintf(`"\s*Float\(\s*%s\s*,\s*%[1]s\s*(,\s*%s\s*)?\)\s*"|FloatS\(\s*%[1]s\s*,\s*%[1]s\s*(,\s*%[2]s\s*)?\)`,
			floatEx,
			intEx),
	)
	result = floatRangeEx.ReplaceAllStringFunc(value, func(match string) string {
		match = strings.Trim(match, t.getCutSet(match, "Float"))
		parts := strings.Split(match, ",")
		if len(parts) < 2 {
			err = errors.New("the range must be in m:n format")
		}
		from, e := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
		if e != nil {
			err = fmt.Errorf("the lower range value must be a valid floating point number: %w", e)
			return ""
		}
		to, e := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		if e != nil {
			err = fmt.Errorf("the upper range value must be a valid floating point number: %w", e)
			return ""
		}

		if from > to {
			err = fmt.Errorf("the upper range value (%f) must be greater than the lower range value (%f)", to, from)
			return ""
		}

		format := "%f"
		if len(parts) >= 3 {
			decimal, e := strconv.ParseUint(strings.TrimSpace(parts[2]), 10, 32)
			if e != nil {
				err = fmt.Errorf("the number of decimal places must be a valid positive integer: %w", e)
				return ""
			}
			format = fmt.Sprintf("%%.%df", decimal)
		}

		return fmt.Sprintf(format, gofakeit.Float64Range(from, to))
	})

	return
}

func (t *Parser) replaceIntNumberGenerators(value string) string {
	intEx := regexp.MustCompile(fmt.Sprintf(`"\s*Int\(%s\)\s*"|IntS\(%[1]s\)`, intPlaceHolderEx))
	return intEx.ReplaceAllStringFunc(value, func(match string) string {
		match = strings.Trim(match, t.getCutSet(match, "Int"))
		return gofakeit.Numerify(match)
	})
}

func (t *Parser) replaceFloatNumberGenerators(value string) string {
	floatEx := regexp.MustCompile(fmt.Sprintf(`"\s*Float\(%s\)\s*"|FloatS\(%[1]s\)`, floatPlaceHolderEx))
	return floatEx.ReplaceAllStringFunc(value, func(match string) string {

		match = strings.Trim(match, t.getCutSet(match, "Float"))
		// The first zero will be replaced in gofakeit.Numerify!
		if len(match) > 0 && match[0] == '0' {
			match = strings.Replace(match, "0", "*^*", 1)
		}
		return strings.Replace(gofakeit.Numerify(match), "*^*", "0", 1)
	})
}

func (*Parser) replaceB64Generators(value string) string {
	base64Ex := regexp.MustCompile(`B64\(.*\)`)
	value = base64Ex.ReplaceAllStringFunc(value, func(match string) string {
		m := gofakeit.Lexify(match[2 : len(match)-1])
		return base64.StdEncoding.EncodeToString([]byte(m))
	})
	return value
}

func (t *Parser) replaceIntRangeGenerators(value string) (result string, err error) {
	intRangeEx := regexp.MustCompile(fmt.Sprintf(`"\s*Int\(\s*%s\s*,\s*%[1]s\s*\)\s*"|IntS\(\s*%[1]s\s*,\s*%[1]s\s*\)`, signedIntEx))
	result = intRangeEx.ReplaceAllStringFunc(value, func(match string) string {
		match = strings.Trim(match, t.getCutSet(match, "Int"))
		parts := strings.Split(match, ",")
		if len(parts) != 2 {
			err = errors.New("the range must be in m:n format")
		}
		from, e := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
		if e != nil {
			err = fmt.Errorf("the lower range value must be a valid integer: %w", e)
			return ""
		}
		to, e := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil {
			err = fmt.Errorf("the upper range value must be a valid integer: %w", e)
			return ""
		}

		if from > to {
			err = fmt.Errorf("the upper range value (%d) must be greater than the lower range value (%d)", to, from)
			return ""
		}

		return strconv.FormatInt(int64(gofakeit.Number(int(from), int(to))), 10)
	})

	return
}

func (t *Parser) replaceTimestampGenerators(value string) (result string, err error) {
	ex := regexp.MustCompile(`(Now|Timestamp)\(.*\)`)
	now := time.Now()
	timeZoneEx := regexp.MustCompile(`(?i)\s*,\s*'.+\s*'`)
	result = ex.ReplaceAllStringFunc(value, func(match string) string {
		var tm time.Time
		if strings.HasPrefix(match, "Now") {
			match = match[4 : len(match)-1]
			tm = now
		} else {
			match = match[10 : len(match)-1]
			tm = gofakeit.Date()
		}
		match = timeZoneEx.ReplaceAllStringFunc(match, func(timezone string) string {
			timezone = strings.Trim(timezone, "', ")
			loc, e := t.parseTimezone(timezone)
			if e != nil {
				err = e
				return ""
			}
			tm = tm.In(loc)
			return ""
		})

		if err != nil {
			return ""
		}

		layout := strings.TrimSpace(strings.ReplaceAll(match, "'", ""))
		if internal.IsEmpty(layout) {
			layout = time.RFC3339
		}
		return tm.Format(layout)
	})
	return
}

func (t *Parser) parseTimezone(timezone string) (*time.Location, error) {
	if internal.IsEmpty(timezone) {
		return nil, errors.New("timezone value cannot be empty")
	}

	loc, err := time.LoadLocation(timezone)
	if err == nil {
		return loc, nil
	}

	// The value might be a fixed offset in UTC±hh:mm format
	timezone = strings.TrimSpace(timezone)
	if !t.fixedOffsetEx.MatchString(timezone) {
		return nil, t.timezoneError(timezone)
	}

	parts := strings.Split(timezone[3:], ":")
	if len(parts) == 0 {
		return nil, t.timezoneError(timezone)
	}

	hours, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, t.timezoneError(timezone)
	}

	var minutes int
	if len(parts) > 1 {
		minutes, err = strconv.Atoi(parts[1])
		if err != nil {
			return nil, t.timezoneError(timezone)
		}
	} else {
		timezone += ":00"
	}
	if hours < 0 {
		minutes *= -1
	}
	return time.FixedZone(timezone, (hours*60*60)+(minutes*60)), nil
}

func (t *Parser) replaceExtraGenerators(value string) string {
	matchers := []struct {
		ex       *regexp.Regexp
		replacer func(match string) string
	}{
		{
			ex: regexp.MustCompile(`Str\([\s?]+\)`),
			replacer: func(match string) string {
				return gofakeit.Lexify(match[4 : len(match)-1])
			},
		},
		{
			ex: regexp.MustCompile(`(Email|EmailAddress)\(\)`),
			replacer: func(match string) string {
				return gofakeit.Email()
			},
		},
		{
			ex: regexp.MustCompile(`IP\(v4\)`),
			replacer: func(match string) string {
				return gofakeit.IPv4Address()
			},
		},
		{
			ex: regexp.MustCompile(`IP\(v6\)`),
			replacer: func(match string) string {
				return gofakeit.IPv6Address()
			},
		},
		{
			ex: regexp.MustCompile(`MacAddress\(\)`),
			replacer: func(match string) string {
				return gofakeit.MacAddress()
			},
		},
		{
			ex: regexp.MustCompile(`FirstName\(\)`),
			replacer: func(match string) string {
				return gofakeit.FirstName()
			},
		},
		{
			ex: regexp.MustCompile(`LastName\(\)`),
			replacer: func(match string) string {
				return gofakeit.LastName()
			},
		},
		{
			ex: regexp.MustCompile(`Name\(\)`),
			replacer: func(match string) string {
				return gofakeit.Name()
			},
		},
		{
			ex: regexp.MustCompile(`NamePrefix\(\)`),
			replacer: func(match string) string {
				return gofakeit.NamePrefix()
			},
		},
		{
			ex: regexp.MustCompile(`NameSuffix\(\)`),
			replacer: func(match string) string {
				return gofakeit.NameSuffix()
			},
		},
		{
			ex: regexp.MustCompile(`Country\(\)`),
			replacer: func(match string) string {
				return gofakeit.Country()
			},
		},
		{
			ex: regexp.MustCompile(`CountryAbr\(\)`),
			replacer: func(match string) string {
				return gofakeit.CountryAbr()
			},
		},
		{
			ex: regexp.MustCompile(`State\(\)`),
			replacer: func(match string) string {
				return gofakeit.State()
			},
		},
		{
			ex: regexp.MustCompile(`StateAbr\(\)`),
			replacer: func(match string) string {
				return gofakeit.StateAbr()
			},
		},
		{
			ex: regexp.MustCompile(`City\(\)`),
			replacer: func(match string) string {
				return gofakeit.City()
			},
		},
		{
			ex: regexp.MustCompile(`Street\(\)`),
			replacer: func(match string) string {
				return gofakeit.Street()
			},
		},
		{
			ex: regexp.MustCompile(`StreetName\(\)`),
			replacer: func(match string) string {
				return gofakeit.StreetName()
			},
		},
		{
			ex: regexp.MustCompile(`StreetPrefix\(\)`),
			replacer: func(match string) string {
				return gofakeit.StreetPrefix()
			},
		},
		{
			ex: regexp.MustCompile(`StreetSuffix\(\)`),
			replacer: func(match string) string {
				return gofakeit.StreetSuffix()
			},
		},
		{
			ex: regexp.MustCompile(`"\s*Bool\(\)\s*"|BoolS\(\)`),
			replacer: func(match string) string {
				return gofakeit.RandString([]string{"true", "false"})
			},
		},
		{
			ex: regexp.MustCompile(`UUID\(\)`),
			replacer: func(match string) string {
				return gofakeit.UUID()
			},
		},
		{
			ex: regexp.MustCompile(`Color\(\)|Colour\(\)`),
			replacer: func(match string) string {
				return gofakeit.Color()
			},
		},
		{
			ex: regexp.MustCompile(`HexColor\(\)|HexColour\(\)`),
			replacer: func(match string) string {
				return gofakeit.HexColor()
			},
		},
		{
			ex: regexp.MustCompile(`CurrencyAbr\(\)`),
			replacer: func(match string) string {
				return gofakeit.CurrencyShort()
			},
		},
		{
			ex: regexp.MustCompile(`Currency\(\)`),
			replacer: func(match string) string {
				return gofakeit.CurrencyLong()
			},
		},
		{
			ex: regexp.MustCompile(`Gender\(\)`),
			replacer: func(match string) string {
				return gofakeit.Gender()
			},
		},
		{
			ex: regexp.MustCompile(`UserAgent\(\)`),
			replacer: func(match string) string {
				return gofakeit.UserAgent()
			},
		},
		{
			ex: regexp.MustCompile(`Username\(\)`),
			replacer: func(match string) string {
				return gofakeit.Username()
			},
		},
		{
			ex: regexp.MustCompile(`URL\(\)`),
			replacer: func(match string) string {
				return gofakeit.URL()
			},
		},
		{
			ex: regexp.MustCompile(`DomainName\(\)`),
			replacer: func(match string) string {
				return gofakeit.DomainName()
			},
		},
		{
			ex: regexp.MustCompile(`DomainSuffix\(\)`),
			replacer: func(match string) string {
				return gofakeit.DomainSuffix()
			},
		},
		{
			ex: regexp.MustCompile(`TimeZoneFull\(\)`),
			replacer: func(match string) string {
				return gofakeit.TimeZoneFull()
			},
		},
		{
			ex: regexp.MustCompile(`TimeZoneAbr\(\)`),
			replacer: func(match string) string {
				return gofakeit.TimeZoneAbv()
			},
		},
		{
			ex: regexp.MustCompile(`TimeZone\(\)`),
			replacer: func(match string) string {
				return gofakeit.TimeZone()
			},
		},
		{
			ex: regexp.MustCompile(`Month\(\)`),
			replacer: func(match string) string {
				return gofakeit.Month()
			},
		},
		{
			ex: regexp.MustCompile(`WeekDay\(\)`),
			replacer: func(match string) string {
				return gofakeit.WeekDay()
			},
		},
		{
			ex: regexp.MustCompile(`HTTPMethod\(\)`),
			replacer: func(match string) string {
				return gofakeit.HTTPMethod()
			},
		},
		{
			ex: regexp.MustCompile(`PetName\(\)`),
			replacer: func(match string) string {
				return gofakeit.PetName()
			},
		},
		{
			ex: regexp.MustCompile(`Animal\(\)`),
			replacer: func(match string) string {
				return gofakeit.Animal()
			},
		},
		{
			ex: regexp.MustCompile(`AnimalType\(\)`),
			replacer: func(match string) string {
				return gofakeit.AnimalType()
			},
		},
		{
			ex: regexp.MustCompile(`FarmAnimal\(\)`),
			replacer: func(match string) string {
				return gofakeit.FarmAnimal()
			},
		},
		{
			ex: regexp.MustCompile(`Cat\(\)`),
			replacer: func(match string) string {
				return gofakeit.Cat()
			},
		},
		{
			ex: regexp.MustCompile(`Dog\(\)`),
			replacer: func(match string) string {
				return gofakeit.Dog()
			},
		},
		{
			ex: regexp.MustCompile(`BeerName\(\)`),
			replacer: func(match string) string {
				return gofakeit.BeerName()
			},
		},
		{
			ex: regexp.MustCompile(`BeerStyle\(\)`),
			replacer: func(match string) string {
				return gofakeit.BeerStyle()
			},
		},
		{
			ex: regexp.MustCompile(`BuzzWord\(\)`),
			replacer: func(match string) string {
				return gofakeit.BuzzWord()
			},
		},
		{
			ex: regexp.MustCompile(`CarMaker\(\)`),
			replacer: func(match string) string {
				return gofakeit.CarMaker()
			},
		},
		{
			ex: regexp.MustCompile(`CarModel\(\)`),
			replacer: func(match string) string {
				return gofakeit.CarModel()
			},
		},
		{
			ex: regexp.MustCompile(`Company\(\)`),
			replacer: func(match string) string {
				return gofakeit.Company()
			},
		},
		{
			ex: regexp.MustCompile(`CompanySuffix\(\)`),
			replacer: func(match string) string {
				return gofakeit.CompanySuffix()
			},
		},
		{
			ex: regexp.MustCompile(`CreditCardCvv\(\)`),
			replacer: func(match string) string {
				return gofakeit.CreditCardCvv()
			},
		},
		{
			ex: regexp.MustCompile(`CreditCardExp\(\)`),
			replacer: func(match string) string {
				return gofakeit.CreditCardExp()
			},
		},
		{
			ex: regexp.MustCompile(`"\s*CreditCardNumber\(\)\s*"|CreditCardNumberS\(\)`),
			replacer: func(match string) string {
				return strconv.Itoa(gofakeit.CreditCardNumber())
			},
		},
		{
			ex: regexp.MustCompile(`"\s*CreditCardNumberLuhn\(\)\s*"|CreditCardNumberLuhnS\(\)`),
			replacer: func(match string) string {
				return strconv.Itoa(gofakeit.CreditCardNumberLuhn())
			},
		},
		{
			ex: regexp.MustCompile(`CreditCardType\(\)`),
			replacer: func(match string) string {
				return gofakeit.CreditCardType()
			},
		},
		{
			ex: regexp.MustCompile(`ProgrammingLanguage\(\)`),
			replacer: func(match string) string {
				return gofakeit.ProgrammingLanguage()
			},
		},
		{
			ex: regexp.MustCompile(`Language\(\)`),
			replacer: func(match string) string {
				return gofakeit.Language()
			},
		},
		{
			ex: regexp.MustCompile(`MimeType\(\)`),
			replacer: func(match string) string {
				return gofakeit.MimeType()
			},
		},
		{
			ex: regexp.MustCompile(`PhoneFormatted\(\)`),
			replacer: func(match string) string {
				return gofakeit.PhoneFormatted()
			},
		},
		{
			ex: regexp.MustCompile(`Phone\(\)`),
			replacer: func(match string) string {
				return gofakeit.Phone()
			},
		},
		{
			ex: regexp.MustCompile(`Sentence\(\s*[0-9]+\s*\)`),
			replacer: func(match string) string {
				param := strings.TrimSpace(match[9 : len(match)-1])
				if param == "" {
					return ""
				}
				count, err := strconv.Atoi(param)
				if err != nil {
					return match
				}
				return gofakeit.Sentence(count)
			},
		},
		// PICK must be the last replacer in the list
		{
			ex: regexp.MustCompile(`"\s*Pick\(.*\)\s*"|PickS\(.*\)`),
			replacer: func(match string) string {
				match = strings.Trim(match, t.getCutSet(match, "Pick"))
				if internal.IsEmpty(match) {
					return match
				}
				list := strings.FieldsFunc(match, func(r rune) bool {
					return r == ',' || r == ' '
				})
				return gofakeit.RandString(list)
			},
		},
	}

	for _, matcher := range matchers {
		value = matcher.ex.ReplaceAllStringFunc(value, matcher.replacer)
	}

	return value
}

func (*Parser) timezoneError(timezone string) error {
	return fmt.Errorf("the timezone value must be either in UTC±hh:mm format or a standard IANA value (eg 'America/New_York'): %s", timezone)
}

func (*Parser) getCutSet(match, fn string) string {
	prefix := fn + "S"
	if strings.Contains(match, prefix) {
		return prefix + "()"
	}
	return fn + "()\" "
}
