package swarm

import "go.uber.org/fx"

var Module = fx.Module("balda_swarm",
	fx.Provide(
		NewTaskService,
		NewAgentRegistry,
		NewAgentAllocator,
		NewCoordinator,
		fx.Annotate(NewMemoryActor, fx.As(new(Actor)), fx.ResultTags(`group:"balda_swarm_actors"`)),
		NewRuntime,
	),
	fx.Invoke(func(*Runtime) {}),
)
