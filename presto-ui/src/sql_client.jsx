import { createRoot } from 'react-dom/client';
import { PageTitle } from "./components/PageTitle";
import 'prismjs/themes/prism-okaidia.css';
import lazy from "./lazy";
import useInactivityMonitor from "./hooks/useInactivityMonitor";

const SQLClientView = lazy('SQLClient');

const InactivityMonitor = () => {
  useInactivityMonitor();
  return null;
};

const title = createRoot(document.getElementById('title'));
title.render(<PageTitle titles={['Cluster Overview', 'Resource Groups', 'SQL Client']} urls={['./index.html', './res_groups.html', './sql_client.html']} current={2} />);

const resourceGroups = createRoot(document.getElementById('sql-client'));
resourceGroups.render(<SQLClientView />);

const inactivityRoot = createRoot(document.body.appendChild(document.createElement('div')));
inactivityRoot.render(<InactivityMonitor />);
