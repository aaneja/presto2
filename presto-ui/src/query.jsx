import React from "react";
import ReactDOM from "react-dom";
import lazy from "./lazy";
import {PageTitle} from "./components/PageTitle";
import useInactivityMonitor from "./hooks/useInactivityMonitor";

const QueryDetail = lazy('QueryDetail');

const InactivityMonitor = () => {
  useInactivityMonitor();
  return null;
};

ReactDOM.render(
    <PageTitle titles={["Query Details"]} />,
    document.getElementById('title')
);

ReactDOM.render(
    <QueryDetail />,
    document.getElementById('query-detail')
);

ReactDOM.render(<InactivityMonitor />, document.body.appendChild(document.createElement('div')));
