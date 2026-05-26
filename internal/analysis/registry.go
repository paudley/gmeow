// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"blackcat.ca/gmeow/internal/contracts"
)

type Registry struct {
	analyzers map[string]Analyzer
}

func NewRegistry(analyzers ...Analyzer) (*Registry, error) {
	registry := &Registry{analyzers: map[string]Analyzer{}}
	for _, analyzer := range analyzers {
		if analyzer == nil {
			return nil, errors.New("analysis analyzer is required")
		}
		spec := analyzer.Spec()
		if err := ValidateSpec(spec); err != nil {
			return nil, err
		}
		key := SpecKey(spec)
		if _, exists := registry.analyzers[key]; exists {
			return nil, fmt.Errorf("duplicate analyzer registration %s", key)
		}
		registry.analyzers[key] = analyzer
	}
	return registry, nil
}

func (registry *Registry) Analyzer(spec contracts.AnalyzerSpec) (Analyzer, bool) {
	if registry == nil {
		return nil, false
	}
	analyzer, ok := registry.analyzers[SpecKey(spec)]
	return analyzer, ok
}

func (registry *Registry) Specs() []contracts.AnalyzerSpec {
	if registry == nil {
		return nil
	}
	specs := make([]contracts.AnalyzerSpec, 0, len(registry.analyzers))
	for _, analyzer := range registry.analyzers {
		specs = append(specs, analyzer.Spec())
	}
	sort.SliceStable(specs, func(left, right int) bool {
		if specs[left].Name != specs[right].Name {
			return specs[left].Name < specs[right].Name
		}
		return specs[left].Version < specs[right].Version
	})
	return specs
}

func SpecKey(spec contracts.AnalyzerSpec) string {
	return strings.TrimSpace(spec.Name) + "\x00" + strings.TrimSpace(spec.Version)
}

func ValidateSpec(spec contracts.AnalyzerSpec) error {
	if strings.TrimSpace(spec.Name) == "" {
		return errors.New("analyzer name is required")
	}
	if strings.TrimSpace(spec.Version) == "" {
		return fmt.Errorf("analyzer %q version is required", spec.Name)
	}
	return nil
}
