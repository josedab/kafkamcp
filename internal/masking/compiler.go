package masking

import (
	"regexp"
	"strings"
)

type ruleCompileSource uint8

const (
	ruleCompileConfig ruleCompileSource = iota
	ruleCompileDynamic
	ruleCompilePolicy
)

type ruleCompileErrorKind uint8

const (
	ruleCompilePatternRequired ruleCompileErrorKind = iota
	ruleCompilePatternTooLong
	ruleCompilePatternUnsafe
	ruleCompilePatternInvalid
	ruleCompileUnknownType
)

type ruleCompileError struct {
	kind  ruleCompileErrorKind
	cause error
}

type ruleCompileInput struct {
	Name      string
	Type      string
	FieldPath string
	Pattern   string
	Replace   string
	Agents    []string
}

func compileMaskingRule(input ruleCompileInput, source ruleCompileSource) (Rule, *ruleCompileError) {
	compiled := Rule{
		Name:      input.Name,
		Type:      input.Type,
		FieldPath: input.FieldPath,
		Replace:   input.Replace,
	}
	if compiled.Replace == "" {
		compiled.Replace = defaultReplacement
	}
	if len(input.Agents) > 0 {
		compiled.Agents = make(map[string]bool, len(input.Agents))
		for _, agent := range input.Agents {
			compiled.Agents[agent] = true
		}
	}

	if source == ruleCompilePolicy {
		pattern, compileErr := compileMaskingPattern(input.Pattern, false, false)
		compiled.Pattern = pattern
		return compiled, compileErr
	}

	switch input.Type {
	case "email":
		compiled.Pattern = emailRegex
	case "ssn":
		compiled.Pattern = ssnRegex
	case "phone":
		compiled.Pattern = phoneRegex
	case "credit_card":
		compiled.Pattern = creditCardRegex
	case "regex":
		pattern, compileErr := compileMaskingPattern(input.Pattern, source == ruleCompileDynamic, source == ruleCompileDynamic)
		compiled.Pattern = pattern
		return compiled, compileErr
	case "ner":
		// NER uses all built-in patterns.
	default:
		if source == ruleCompileDynamic {
			return Rule{}, &ruleCompileError{kind: ruleCompileUnknownType}
		}
	}
	return compiled, nil
}

func compileMaskingPattern(pattern string, requirePattern, strictFlags bool) (*regexp.Regexp, *ruleCompileError) {
	if pattern == "" {
		if requirePattern {
			return nil, &ruleCompileError{kind: ruleCompilePatternRequired}
		}
		return nil, nil
	}
	if len(pattern) > maxRegexPatternLength {
		return nil, &ruleCompileError{kind: ruleCompilePatternTooLong}
	}
	if idx := strings.Index(pattern, "(?"); idx >= 0 {
		allowed := strings.Contains(pattern, "(?i)")
		if strictFlags {
			allowed = idx == 0 && strings.HasPrefix(pattern, "(?i)") && !strings.Contains(pattern[4:], "(?")
		}
		if !allowed {
			return nil, &ruleCompileError{kind: ruleCompilePatternUnsafe}
		}
	}
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return nil, &ruleCompileError{kind: ruleCompilePatternInvalid, cause: err}
	}
	return compiled, nil
}
