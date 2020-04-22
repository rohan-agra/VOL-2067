/*
 * Copyright 2020-present Open Networking Foundation

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package common

import (
	"context"
	"github.com/golang/protobuf/ptypes"
	"github.com/opencord/voltha-lib-go/v3/pkg/db"
	"github.com/opencord/voltha-lib-go/v3/pkg/kafka"
	mocks "github.com/opencord/voltha-lib-go/v3/pkg/mocks/kafka"
	ic "github.com/opencord/voltha-protos/v3/go/inter_container"
	"github.com/opencord/voltha-protos/v3/go/voltha"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const (
	embedEtcdServerPort = 2379
	embedEtcdServerHost = "localhost"
	defaultTimeout      = 1
	defaultPathPrefix   = "Prefix"
)

func TestNewAdapterProxy(t *testing.T) {

	var mockKafkaIcProxy = &mocks.MockKafkaICProxy{
		InvokeRpcSpy: mocks.InvokeRpcSpy{
			Calls:    make(map[int]mocks.InvokeRpcArgs),
			Response: &voltha.Device{Id: "testDeviceId"},
		},
	}
	backend := db.NewBackend("etcd", embedEtcdServerHost, embedEtcdServerPort, defaultTimeout, defaultPathPrefix)
	adapter := NewAdapterProxy(mockKafkaIcProxy, "testAdapterTopic", "testCoreTopic", backend)

	assert.NotNil(t, adapter)
}

func TestSendInterAdapterMessage(t *testing.T) {

	var mockKafkaIcProxy = &mocks.MockKafkaICProxy{
		InvokeRpcSpy: mocks.InvokeRpcSpy{
			Calls:    make(map[int]mocks.InvokeRpcArgs),
			Response: &voltha.Device{Id: "testDeviceId"},
		},
	}

	backend := db.NewBackend("etcd", embedEtcdServerHost, embedEtcdServerPort, defaultTimeout, defaultPathPrefix)

	adapter := NewAdapterProxy(mockKafkaIcProxy, "testAdapterTopic", "testCoreTopic", backend)

	adapter.endpointMgr = mocks.NewEndpointManager()

	delGemPortMsg := &ic.InterAdapterDeleteGemPortMessage{UniId: 1, TpPath: "tpPath", GemPortId: 2}

	err := adapter.SendInterAdapterMessage(context.TODO(), delGemPortMsg, ic.InterAdapterMessageType_DELETE_GEM_PORT_REQUEST, "Adapter1", "Adapter2", "testDeviceId", "testProxyDeviceId", "testMessage")

	header := &ic.InterAdapterHeader{
		Id:            "testMessage",
		Type:          ic.InterAdapterMessageType_DELETE_GEM_PORT_REQUEST,
		FromTopic:     "Adapter1",
		ToTopic:       "Adapter2",
		ToDeviceId:    "testDeviceId",
		ProxyDeviceId: "testProxyDeviceId",
		Timestamp:     time.Now().Unix(),
	}

	marshalledMsg, er := ptypes.MarshalAny(delGemPortMsg)

	iaMsg := &ic.InterAdapterMessage{
		Header: header,
		Body:   marshalledMsg,
	}

	assert.Equal(t, er, nil)
	assert.Equal(t, err, nil)

	call := mockKafkaIcProxy.InvokeRpcSpy.Calls[1]

	assert.Equal(t, mockKafkaIcProxy.InvokeRpcSpy.CallCount, 1)

	assert.Equal(t, call.Rpc, "process_inter_adapter_message")
	assert.Equal(t, call.ToTopic, &kafka.Topic{Name: "Adapter2"})
	assert.Equal(t, call.ReplyToTopic, &kafka.Topic{Name: "Adapter1"})
	assert.Equal(t, call.WaitForResponse, true)
	assert.Equal(t, call.Key, "testProxyDeviceId")
	assert.Equal(t, call.KvArgs[0], &kafka.KVArg{Key: "msg", Value: iaMsg})
}
