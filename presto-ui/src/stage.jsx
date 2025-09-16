import React from "react";
import ReactDOM from "react-dom";
import {PageTitle} from "./components/PageTitle";
import lazy from "./lazy";
import useInactivityMonitor from "./hooks/useInactivityMonitor";

const StageDetail = lazy('StageDetail');

const InactivityMonitor = () => {
  useInactivityMonitor();
  return null;
};

ReactDOM.render(
    <PageTitle titles={["Query Details"]} />,
    document.getElementById('title')
);

ReactDOM.render(
    <StageDetail />,
    document.getElementById('stage-performance-header')
);

ReactDOM.render(<InactivityMonitor />, document.body.appendChild(document.createElement('div')));
