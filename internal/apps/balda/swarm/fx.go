package swarm

import "go.uber.org/fx"

var Module = fx.Module("balda_swarm",
	fx.Provide(
		fx.Annotate(NewEmbeddedBus, fx.As(new(WakeBus))),
		NewMailboxService,
		NewCoordinator,
		NewRuntime,
	),
	fx.Invoke(func(*Runtime) {}),
)
