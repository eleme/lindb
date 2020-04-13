import { Layout } from 'antd'
import classNames from 'classnames'
import * as React from 'react'

const { Footer: AntDFooter } = Layout

interface FooterProps {
  sider?: boolean
}

interface FooterStatus {
}

export default class Footer extends React.Component<FooterProps, FooterStatus> {
  constructor(props: FooterProps) {
    super(props)
    this.state = {}
  }

  render() {
    const { sider } = this.props
    const cls = classNames('lindb-footer', {
      'sider': sider
    })

    return (
      <AntDFooter className={cls}>
        Copyright &copy; 2020 LinDB
      </AntDFooter>
    )
  }
}