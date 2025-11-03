import React from "react";
import ReactDOM from "react-dom";
import lazy from "./lazy";
import {PageTitle} from "./components/PageTitle";
import useInactivityMonitor from "./hooks/useInactivityMonitor";

const WorkerStatus = lazy('WorkerStatus');
const WorkerThreadList = lazy('WorkerThreadList');

const InactivityMonitor = () => {
  useInactivityMonitor();
  return null;
};

ReactDOM.render(
    <PageTitle titles={["Worker Status"]} />,
    document.getElementById('title')
);

ReactDOM.render(
    <WorkerStatus />,
    document.getElementById('worker-status')
);

ReactDOM.render(
    <WorkerThreadList />,
    document.getElementById('worker-threads')
);

ReactDOM.render(<InactivityMonitor />, document.body.appendChild(document.createElement('div')));
