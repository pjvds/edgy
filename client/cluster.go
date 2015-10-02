package client

import (
	"errors"
	"fmt"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/pjvds/edgy/api"
	"github.com/pjvds/tidy"
)

var logger = tidy.GetLogger()

type ClusterBuilder struct {
	nodes []Node
}

type Cluster struct {
	nodes []Node
}

func (this Cluster) Consume(topics ...string) (Consumer, error) {
	consumers := make([]Consumer, 0, len(this.nodes)*len(topics))

	for _, node := range this.nodes {
		for _, topic := range topics {
			consumer, err := node.ConsumeTopic(topic, false)

			if err != nil {
				// TODO: close consumers
				return nil, err
			}

			consumers = append(consumers, consumer)
		}
	}

	if len(consumers) == 1 {
		return consumers[0], nil
	}
	return MergeConsumers(consumers...), nil
}

func (this ClusterBuilder) Node(name, ip string, partition int32) ClusterBuilder {
	if len(name) == 0 {
		panic("missing name value")
	}
	if len(ip) == 0 {
		panic("missing name value")
	}

	this.nodes = append(this.nodes, Node{
		Name:      name,
		IP:        ip,
		Partition: partition,
	})

	return this
}

type Node struct {
	Name      string
	IP        string
	Partition int32

	client api.EdgyClient
}

func (this Node) ConsumeTopic(topic string, continuous bool) (Consumer, error) {
	return NewTopicPartitionConsumer(this.IP, topic, this.Partition, continuous)
}

func NewCluster() ClusterBuilder {
	return ClusterBuilder{
		nodes: make([]Node, 0, 10),
	}
}

func (this Cluster) Partitions() int {
	return len(this.nodes)
}

func (this ClusterBuilder) MustBuild() Cluster {
	cluster, err := this.Build()
	if err != nil {
		panic(err)
	}
	return cluster
}

func (this Cluster) GetNode(partition int32) (Node, bool) {
	p := int(partition)
	if p < 0 {
		return Node{}, false
	}
	if p > len(this.nodes)-1 {
		return Node{}, false
	}

	return this.nodes[p], true
}

func (this ClusterBuilder) Build() (Cluster, error) {
	cluster := Cluster{
		nodes: make([]Node, len(this.nodes)),
	}

	if len(this.nodes) == 0 {
		return cluster, errors.New("no nodes in cluster")
	}

	nodes := make(map[int32]Node, len(this.nodes))

	for _, node := range this.nodes {
		if duplicate, ok := nodes[node.Partition]; ok {
			return cluster, fmt.Errorf("duplicate partition %v, node %v and %v", node.Partition, node.Name, duplicate.Name)
		}

		nodes[node.Partition] = node
	}

	for p := int32(0); p < int32(len(this.nodes)); p++ {
		if node, ok := nodes[p]; !ok {
			return cluster, fmt.Errorf("missing partition %v", p)
		} else {
			logger.With("host", node.IP).Debug("connecting to host")

			connection, err := grpc.Dial(node.IP, grpc.WithInsecure())
			if err != nil {
				// TODO: close connections
				return cluster, err
			}

			client := api.NewEdgyClient(connection)
			if _, err := client.Ping(context.Background(), &api.PingRequest{}); err != nil {
				// TODO: close connections
				return cluster, err
			}
			logger.With("host", node.IP).Debug("connected to host")

			node.client = client
			cluster.nodes[p] = node
		}
	}

	return cluster, nil
}
