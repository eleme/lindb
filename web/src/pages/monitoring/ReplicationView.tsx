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
import {
  Card,
  Form,
  Button,
  RadioGroup,
  Radio,
  Space,
} from "@douyinfe/semi-ui";
import { IconRefresh } from "@douyinfe/semi-icons";
import {
  DatabaseView,
  Icon,
  LinSelect,
  MemoryDatabaseView,
  ReplicaView,
  StatusTip,
} from "@src/components";
import { SQL } from "@src/constants";
import { ReplicaState, MemoryDatabaseState, StorageState } from "@src/models";
import { ExecService } from "@src/services";
import { StateKit } from "@src/utils";
import * as _ from "lodash-es";
import React from "react";
import { URLStore } from "@src/stores";
import { useQuery } from "@tanstack/react-query";
import { useParams } from "@src/hooks";
import { observer } from "mobx-react-lite";

enum StateType {
  WAL = "wal",
  Memory = "memory",
}

const ReplicationStatus: React.FC = observer(() => {
  const { db, type } = useParams(["db", "type"]);
  const { isInitialLoading, isFetching, isError, error, data } = useQuery(
    ["show_replication", db, type, URLStore.forceChanged],
    async () => {
      const storages = await ExecService.exec<StorageState[]>({
        sql: SQL.ShowStorageAliveNodes,
      });
      const databases = StateKit.getDatabaseList(storages);
      const database = _.find(databases, { name: db });
      if (!database) {
        return null;
      }
      if (type === StateType.Memory) {
        const memoryDatabase = await ExecService.exec<MemoryDatabaseState>({
          sql: `show memory database where storage='${database.storage.name}' and database='${db}'`,
        });
        return { database: database, memoryDatabaseState: memoryDatabase };
      } else {
        const replicaState = await ExecService.exec<ReplicaState>({
          sql: `show replication where storage='${database.storage.name}' and database='${db}'`,
        });
        return { database: database, replicaState: replicaState };
      }
    },
    {
      enabled: !_.isEmpty(db),
    }
  );
  if (isInitialLoading || isFetching || isError || !data) {
    return (
      <StatusTip
        isLoading={isInitialLoading || isFetching}
        isError={isError}
        error={error}
        isEmpty={!data}
        style={{ marginTop: 100, marginBottom: 100 }}
      />
    );
  }
  return (
    <>
      <DatabaseView
        liveNodes={_.get(data, "database.storage.liveNodes", {})}
        storage={_.get(data, "database.storage", {})}
        databaseName={_.get(data, "database.name")}
      />
      <div style={{ marginTop: 12 }}>
        {type === StateType.Memory ? (
          <MemoryDatabaseView
            liveNodes={_.get(data, "database.storage.liveNodes", {})}
            state={_.get(data, "memoryDatabaseState", {})}
          />
        ) : (
          <ReplicaView
            liveNodes={_.get(data, "database.storage.liveNodes", {})}
            state={_.get(data, "replicaState", {})}
          />
        )}
      </div>
    </>
  );
});

const ReplicationView: React.FC = () => {
  const { type } = useParams(["type"]);
  return (
    <>
      <Card style={{ marginBottom: 12 }} bodyStyle={{ padding: 12 }}>
        <Form
          style={{ paddingTop: 0, paddingBottom: 0 }}
          wrapperCol={{ span: 12 }}
          layout="horizontal"
        >
          <LinSelect
            field="db"
            label="Database"
            loader={() =>
              ExecService.exec<StorageState[]>({
                sql: SQL.ShowStorageAliveNodes,
              }).then((storages) => {
                const databases = StateKit.getDatabaseList(storages);
                return _.map(databases, (db: any) => {
                  return { label: db.name, value: db.name };
                });
              })
            }
            clearKeys={["shard", "family"]}
          />
          <RadioGroup
            style={{ marginRight: 16 }}
            defaultValue={type || "wal"}
            buttonSize="small"
            type="button"
            onChange={(e) =>
              URLStore.changeURLParams({ params: { type: e.target.value } })
            }
          >
            <Radio value="wal" style={{ marginTop: 4, padding: "2px 8px" }}>
              <Space align="center">
                <Icon icon="iconbx-git-repo-forked" style={{ fontSize: 14 }} />
                <div>WAL</div>
              </Space>
            </Radio>
            <Radio value="memory" style={{ marginTop: 4, padding: "2px 8px" }}>
              <Space align="center">
                <Icon icon="icondatabase" style={{ fontSize: 14 }} />
                <div>Memory</div>
              </Space>
            </Radio>
          </RadioGroup>
          <Button
            icon={<IconRefresh />}
            onClick={() => {
              URLStore.changeURLParams({
                forceChange: true,
              });
            }}
          />
        </Form>
      </Card>
      <ReplicationStatus />
    </>
  );
};

export default ReplicationView;
