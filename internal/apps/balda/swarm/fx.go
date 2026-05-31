package swarm

import (
	"github.com/normahq/balda/internal/apps/balda/memory"
	"go.uber.org/fx"
)

var Module = fx.Module("balda_swarm",
	fx.Provide(
		NewTaskService,
		NewEventProjector,
		fx.Annotate(
			func(memoryStore *memory.Store) Actor {
				return memoryActor{memoryStore: memoryStore}
			},
			fx.As(new(Actor)),
			fx.ResultTags(`group:"balda_swarm_actors"`),
		),
		NewRuntime,
	),
	fx.Invoke(func(*EventProjector) {}),
	fx.Invoke(func(*Runtime) {}),
)
