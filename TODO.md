Storage resolution during production startup should use an env configuration
library for reading and applying configuration of the desired storage system
(passing along configuration to the resolver.Resolve or completely replacing
it).

Storage loss is undetected. The store is marked lost only when an operation
fails; the additions subscription's loss is swallowed by go-redis's channel
reader. Fix: Listen drives pubSub.Receive itself so the broken connection is the
event.

Open sub-question: a silent partition still needs a periodic PING, a timer. What
grpcd does once it knows is unsettled: the code and README hold streams and
report NOT_SERVING; you recall shutting down. Downstream of 2.
