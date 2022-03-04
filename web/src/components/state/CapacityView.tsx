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
import React from "react";
import { Progress, Descriptions, Typography } from "@douyinfe/semi-ui";
import { transformPercent, transformBytes } from "@src/utils";

const { Text } = Typography;

interface CapacityViewProps {
  percent: number;
  total: number;
  free: number;
  used: number;
}

export default function CapacityView(props: CapacityViewProps) {
  const { percent, total, free, used } = props;
  return (
    <>
      <div style={{ width: "70%", marginBottom: 8 }}>
        <Progress
          motion={false}
          className="lin-stats"
          percent={percent}
          stroke="#fc8800"
          size="large"
          format={(val) => transformPercent(val)}
          showInfo={true}
        />
      </div>
      <Descriptions
        row
        size="small"
        data={[
          {
            key: "Total",
            value: <Text link>{transformBytes(total)}</Text>,
          },
          {
            key: "Used",
            value: <Text type="warning">{transformBytes(used)}</Text>,
          },
          {
            key: "Free",
            value: <Text type="success">{transformBytes(free)}</Text>,
          },
        ]}
      />
    </>
  );
}
