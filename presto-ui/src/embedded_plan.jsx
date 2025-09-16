import React from "react";
import ReactDOM from "react-dom";
import {getFirstParameter} from "./utils";
import lazy from "./lazy";
import useInactivityMonitor from "./hooks/useInactivityMonitor";

const LivePlan = lazy('LivePlan');

const InactivityMonitor = () => {
  useInactivityMonitor();
  return null;
};

ReactDOM.render(
    <LivePlan queryId={getFirstParameter(window.location.search)} isEmbedded={true}/>,
    document.getElementById('live-plan-container')
);

ReactDOM.render(<InactivityMonitor />, document.body.appendChild(document.createElement('div')));
