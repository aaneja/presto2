import React from "react";
import ReactDOM from "react-dom";
import useInactivityMonitor from "./hooks/useInactivityMonitor";

const InactivityMonitor = () => {
  useInactivityMonitor();
  return null;
};

ReactDOM.render(<InactivityMonitor />, document.body.appendChild(document.createElement('div')));
