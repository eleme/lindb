/*
Licensed to LinDB under one or more contributor
license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright
ownership. LinDB licenses this file to you under
the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
 
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
import { NodeView, MasterView, StorageView } from "@src/components";
import React from "react";
import { useStorage, useAliveState } from "@src/hooks";
import { StateMetricName, StateRoleName } from "@src/constants";

// must define outside function component, if defie in component maybe endless loop.
const aliveQueryParams = {
  role: StateRoleName.Broker,
};
const brokerStateParams = {
  names: [StateMetricName.CPU, StateMetricName.Memory],
  role: StateRoleName.Broker,
};

export default function Overview() {
  const { loading, storages } = useStorage();
  const { aliveState: liveNodes, loading: nodeLoading } =
    useAliveState(aliveQueryParams);
  return (
    <>
      <MasterView />
      <NodeView
        title="Broke Live Nodes"
        loading={nodeLoading}
        nodes={liveNodes}
        stateParams={brokerStateParams}
        style={{ marginTop: 12, marginBottom: 12 }}
      />
      <StorageView loading={loading} storages={storages || []} />
    </>
  );
}
