import {Layout} from 'antd'
import ChartTooltip from 'components/metric/ChartTooltip'
import Database from 'containers/admin/Database'
import Storage from 'containers/admin/Storage'
import OverviewPage from 'containers/home/Overview'
import StorageClusterDetailPage from 'containers/home/StorageClusterDetail'
import Footer from 'containers/layout/Footer'
import Header from 'containers/layout/Header'
import SiderMenu from 'containers/layout/SiderMenu'
import Runtime from 'containers/monitoring/Runtime'
import MonitoringConcurrent from 'containers/monitoring/Concurrent'
import MonitoringQuery from 'containers/monitoring/Query'
import System from 'containers/monitoring/System'
import MonitoringStorage from 'containers/monitoring/Storage'
import MonitoringBroker from 'containers/monitoring/Broker'
import MonitoringTraffic from 'containers/monitoring/Traffic'
import MonitoringKV from 'containers/monitoring/KV'
import MonitoringReplica from 'containers/monitoring/Replica'
import SearchPage from 'containers/query/MetricDataSearch'
import * as React from 'react'
import {Route, Switch} from 'react-router-dom'

const { Content: AntDContent } = Layout

interface ContentProps {
}

interface ContentStatus {
}

export default class Content extends React.Component<ContentProps, ContentStatus> {
  constructor(props: ContentProps) {
    super(props)
    this.state = {}
  }

  render() {
    return (
      <Layout className="lindb-sider-layout">
        {/* Sider Bar Menu */}
        <SiderMenu />

        {/* Content Area */}
        <Layout className="lindb-layout">
          <AntDContent className="lindb-content-container">
            <Header />

            <ChartTooltip />

            <Switch>
              <Route exact={true} path="/" component={OverviewPage} />
              <Route exact={true} path="/storage/cluster/:clusterName" component={StorageClusterDetailPage} />
              <Route exact={true} path="/search" component={SearchPage} />
              <Route exact={true} path="/monitoring/system" component={System} />
              <Route exact={true} path="/monitoring/runtime" component={Runtime} />
              <Route exact={true} path="/monitoring/broker" component={MonitoringBroker}/>
              <Route exact={true} path="/monitoring/storage" component={MonitoringStorage} />
              <Route exact={true} path="/monitoring/concurrent" component={MonitoringConcurrent} />
              <Route exact={true} path="/monitoring/query" component={MonitoringQuery} />
              <Route exact={true} path="/monitoring/traffic" component={MonitoringTraffic} />
              <Route exact={true} path="/monitoring/kv" component={MonitoringKV} />
              <Route exact={true} path="/monitoring/replica" component={MonitoringReplica} />
              <Route exact={true} path="/metadata/storage" component={Storage} />
              <Route exact={true} path="/metadata/database" component={Database} />
            </Switch>
          </AntDContent>

          <Footer sider={true} />
        </Layout>
      </Layout>
    )
  }
}