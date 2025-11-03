import React from "react";
import ReactDOM from "react-dom";
import lazy from "./lazy";
import { PageTitle } from "./components/PageTitle";
import useInactivityMonitor from "./hooks/useInactivityMonitor";

const ClusterHUD = lazy('ClusterHUD');
const QueryList = lazy('QueryList');

const InactivityMonitor = () => {
  useInactivityMonitor();
  return null;
};

ReactDOM.render(
    <PageTitle titles={['Cluster Overview', 'Resource Groups', 'SQL Client']} urls={['./index.html', 'res_groups.html', 'sql_client.html']} current={0}/>,
    document.getElementById('title')
);

ReactDOM.render(
    <ClusterHUD />,
    document.getElementById('cluster-hud')
);

ReactDOM.render(
    <QueryList />,
    document.getElementById('query-list')
);

ReactDOM.render(<InactivityMonitor />, document.body.appendChild(document.createElement('div')));
