import React from "react";
import ReactDOM from "react-dom";
import lazy from "./lazy";
import {PageTitle} from "./components/PageTitle";
import {getFirstParameter} from "./utils";
import useInactivityMonitor from "./hooks/useInactivityMonitor";

const LivePlan = lazy('LivePlan');

const InactivityMonitor = () => {
  useInactivityMonitor();
  return null;
};

ReactDOM.render(
    <PageTitle titles={["Query Details"]} />,
    document.getElementById('title')
);

ReactDOM.render(
    <LivePlan queryId={getFirstParameter(window.location.search)} isEmbedded={false}/>,
    document.getElementById('live-plan-container')
);

ReactDOM.render(<InactivityMonitor />, document.body.appendChild(document.createElement('div')));
