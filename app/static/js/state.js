'use strict';

const state = {
  currentView:    'dashboard',
  charts:         {},
  processesData:  [],
  blocksData:     [],
  bottlenecksData: null,
  issuesData:     null,
};

let _sortState = {};   // { [containerId]: { key, dir } }
let _dashLoaded = false;
