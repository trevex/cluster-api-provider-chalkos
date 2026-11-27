# cluster-api-provider-chalkos

Cluster API bootstrap and control-plane providers for [chalkos](https://github.com/trevex/chalkos),
the API-driven, immutable NixOS-based Linux distribution. The bootstrap provider turns a Machine's
chalkos node into a Kubernetes node; the control-plane provider turns a set of Machines into a
control plane, keeps its kubeconfig Secret current, and scales it the way Cluster API scales any
control plane. See [`lab/`](lab/) for a KubeVirt-based lab that runs both providers against
chalkos virtual machines end to end.
