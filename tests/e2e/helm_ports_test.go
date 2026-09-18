package e2e_test

// One rule, across every chart that publishes an HTTP port: the port the
// manifests name must be the port the process inside the container actually
// binds.
//
// The grafana chart broke it. Its containerPort and its readiness probe were
// both derived from service.port — so `--set service.port=3100` moved both —
// while Grafana itself kept listening on 3000, because nothing told it
// otherwise. The chart rendered, `helm lint` passed, `helm template` produced
// valid YAML, and every existing test here stayed green; the failure surfaced
// only as `helm install --wait` timing out against a probe pointed at a port
// nothing was bound to. deployments/helm/README.md documents service.port as an
// overridable value for that chart, which is what makes it a real path rather
// than a hypothetical one.
//
// Nothing cluster-free can observe a listen() call, so "the port the process
// binds" is read from the one place a manifest determines it: the setting each
// image reads at startup. That is the chart-specific half below; everything
// else is the same for all three.

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// listenPortSource says how each chart tells its container where to listen, and
// resolves it from the rendered objects. A chart whose image has a fixed
// default and no override returns that default, with the reason recorded.
type listenPortSource struct {
	chart string
	// extraArgs are the --set values the chart needs in order to render at all.
	extraArgs []string
	// how names the mechanism, for failure messages.
	how string
	// listenPort reads the port the container process will bind.
	listenPort func(t *testing.T, objs []k8sObject) int
}

// grafanaListenPort reads GF_SERVER_HTTP_PORT, which is the only thing that
// moves Grafana's listener.
func grafanaListenPort(t *testing.T, objs []k8sObject) int {
	t.Helper()
	const key = "GF_SERVER_HTTP_PORT"
	for _, o := range objs {
		for _, c := range o.Spec.Template.Spec.Containers {
			for _, e := range c.Env {
				if e.Name != key {
					continue
				}
				n, err := strconv.Atoi(e.Value)
				if err != nil {
					t.Fatalf("container %q sets %s=%q, which is not a port number", c.Name, key, e.Value)
				}
				return n
			}
		}
	}
	t.Fatalf("no container sets %s, so Grafana listens on its built-in 3000 no matter what the Service, the containerPort and the probes say", key)
	return 0
}

// backendListenPort reads OBS_HTTP_ADDR out of the ConfigMap the StatefulSet
// consumes with envFrom. templates/configmap.yaml derives it from service.port
// and rejects an explicit override, which is the pattern the grafana chart was
// missing.
func backendListenPort(t *testing.T, objs []k8sObject) int {
	t.Helper()
	const key = "OBS_HTTP_ADDR"
	for _, o := range objs {
		if o.Kind != "ConfigMap" {
			continue
		}
		addr, ok := o.Data[key]
		if !ok {
			continue
		}
		_, portStr, found := strings.Cut(addr, ":")
		if !found {
			t.Fatalf("ConfigMap %s sets %s=%q, which names no port", o.Metadata.Name, key, addr)
		}
		n, err := strconv.Atoi(portStr)
		if err != nil {
			t.Fatalf("ConfigMap %s sets %s=%q, whose port is not a number", o.Metadata.Name, key, addr)
		}
		return n
	}
	t.Fatalf("no ConfigMap sets %s, so the server listens on its own default regardless of service.port", key)
	return 0
}

// prometheusListenPort: this chart passes no --web.listen-address, so the
// container binds prom/prometheus's built-in 9090, and the manifest hardcodes
// that same number as its containerPort. service.port therefore moves only the
// Service's front port, which targetPort: http resolves across correctly — a
// legitimate shape, and one this test has to accept rather than demand an env
// var the image does not read.
func prometheusListenPort(t *testing.T, _ []k8sObject) int {
	t.Helper()
	return 9090
}

var listenPortSources = []listenPortSource{
	{
		chart:      grafanaChart,
		extraArgs:  []string{"admin.password=port-test"},
		how:        "the GF_SERVER_HTTP_PORT env var",
		listenPort: grafanaListenPort,
	},
	{
		chart:      backendChart,
		how:        "OBS_HTTP_ADDR in the chart's ConfigMap",
		listenPort: backendListenPort,
	},
	{
		chart:      prometheusChart,
		how:        "prom/prometheus's built-in 9090, since the chart passes no --web.listen-address",
		listenPort: prometheusListenPort,
	},
}

// resolvePort turns a targetPort or probe port — a name or a number — into the
// containerPort it refers to.
func resolvePort(t *testing.T, objs []k8sObject, v any) (int, error) {
	t.Helper()
	switch p := v.(type) {
	case int:
		return p, nil
	case string:
		for _, o := range objs {
			for _, c := range o.Spec.Template.Spec.Containers {
				for _, cp := range c.Ports {
					if cp.Name == p {
						return cp.ContainerPort, nil
					}
				}
			}
		}
		return 0, fmt.Errorf("port name %q matches no containerPort in any pod template; kubelet and kube-proxy would both fail to resolve it", p)
	case nil:
		return 0, fmt.Errorf("no port given")
	default:
		return 0, fmt.Errorf("port %v has unexpected type %T", v, v)
	}
}

// TestChartServicePortReachesTheProcess renders each chart at its default
// service.port and at an off-default one, and requires the Service, the
// containerPort, every httpGet probe and the container's own listen setting to
// agree. The off-default render is the load-bearing half: at the default every
// number in the grafana chart happened to coincide with Grafana's own, which is
// precisely why the bug was invisible.
func TestChartServicePortReachesTheProcess(t *testing.T) {
	for _, src := range listenPortSources {
		t.Run(strings.TrimPrefix(src.chart, "../../deployments/helm/"), func(t *testing.T) {
			for _, override := range []string{"", "18321"} {
				name := "default service.port"
				args := src.extraArgs
				if override != "" {
					name = "service.port=" + override
					args = append(append([]string{}, src.extraArgs...), "service.port="+override)
				}
				t.Run(name, func(t *testing.T) {
					objs := render(t, src.chart, args...)
					listen := src.listenPort(t, objs)

					// Every Service must route to the listening port, and must
					// publish the port service.port names — the second half is
					// what makes the override render meaningful rather than a
					// re-run of the default.
					var services int
					for _, o := range objs {
						if o.Kind != "Service" {
							continue
						}
						for _, p := range o.Spec.Ports {
							services++
							if override != "" {
								want, err := strconv.Atoi(override)
								if err != nil {
									t.Fatalf("test override %q is not a number", override)
								}
								if p.Port != want {
									t.Errorf("Service %s publishes port %d with service.port=%s; that value does not own the Service port",
										o.Metadata.Name, p.Port, override)
								}
							}
							got, err := resolvePort(t, objs, p.TargetPort)
							if err != nil {
								t.Errorf("Service %s port %d: %v", o.Metadata.Name, p.Port, err)
								continue
							}
							if got != listen {
								t.Errorf("Service %s routes port %d to container port %d, but the container listens on %d (set via %s). Traffic through this Service reaches nothing.",
									o.Metadata.Name, p.Port, got, listen, src.how)
							}
						}
					}
					if services == 0 {
						t.Fatal("no Service ports were checked; the render must have changed shape")
					}

					// Every httpGet probe must hit the listening port. This is
					// the assertion the grafana chart failed: a probe on the
					// wrong port fails every pod forever.
					var probes int
					for _, o := range objs {
						for _, c := range o.Spec.Template.Spec.Containers {
							for label, pr := range map[string]*probe{
								"startupProbe":   c.StartupProbe,
								"readinessProbe": c.ReadinessProbe,
								"livenessProbe":  c.LivenessProbe,
							} {
								if pr == nil || pr.HTTPGet == nil {
									continue
								}
								probes++
								got, err := resolvePort(t, objs, pr.HTTPGet.Port)
								if err != nil {
									t.Errorf("container %q %s: %v", c.Name, label, err)
									continue
								}
								if got != listen {
									t.Errorf("container %q %s probes port %d, but the container listens on %d (set via %s). The pod never becomes ready, and `helm install --wait` times out.",
										c.Name, label, got, listen, src.how)
								}
							}
						}
					}
					if probes == 0 {
						t.Fatal("no httpGet probes were checked; the render must have changed shape")
					}
				})
			}
		})
	}
}
