import { PageTitle } from "./components/PageTitle";
import { createRoot } from 'react-dom/client';
import lazy from "./lazy";
import useInactivityMonitor from "./hooks/useInactivityMonitor";

const Splits = lazy('Splits');

const InactivityMonitor = () => {
  useInactivityMonitor();
  return null;
};

const title = createRoot(document.getElementById('title'));
title.render(<PageTitle titles={["Timeline"]} />);

const timeline = createRoot(document.getElementById('timeline'));
timeline.render(<Splits />);

const inactivityRoot = createRoot(document.body.appendChild(document.createElement('div')));
inactivityRoot.render(<InactivityMonitor />);
