// Package contracts embeds versioned schemas. It contains no runtime services.
package contracts

import "embed"

//go:embed manifest/*.json worker/*.json common/*.json
var Files embed.FS
