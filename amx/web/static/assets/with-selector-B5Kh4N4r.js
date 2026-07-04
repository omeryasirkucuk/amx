import{d as v}from"./index-BskO4Aqz.js";import{r as k,g as M}from"./query-fC9Hhka3.js";/**
 * @license lucide-react v0.577.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const _=[["path",{d:"M6 12h9a4 4 0 0 1 0 8H7a1 1 0 0 1-1-1V5a1 1 0 0 1 1-1h7a4 4 0 0 1 0 8",key:"mg9rjx"}]],Y=v("bold",_);/**
 * @license lucide-react v0.577.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const b=[["circle",{cx:"12",cy:"12",r:"10",key:"1mglay"}],["path",{d:"M12 6v6l4 2",key:"mmk7yg"}]],Z=v("clock",b);/**
 * @license lucide-react v0.577.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const g=[["circle",{cx:"12",cy:"12",r:"1",key:"41hilf"}],["circle",{cx:"19",cy:"12",r:"1",key:"1wjl8i"}],["circle",{cx:"5",cy:"12",r:"1",key:"1pcz8c"}]],q=v("ellipsis",g);/**
 * @license lucide-react v0.577.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const V=[["path",{d:"M3 5h.01",key:"18ugdj"}],["path",{d:"M3 12h.01",key:"nlz23k"}],["path",{d:"M3 19h.01",key:"noohij"}],["path",{d:"M8 5h13",key:"1pao27"}],["path",{d:"M8 12h13",key:"1za7za"}],["path",{d:"M8 19h13",key:"m83p4d"}]],ee=v("list",V);var x={exports:{}},$={},j={exports:{}},w={};/**
 * @license React
 * use-sync-external-store-shim.production.js
 *
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */var i=k;function z(e,t){return e===t&&(e!==0||1/e===1/t)||e!==e&&t!==t}var D=typeof Object.is=="function"?Object.is:z,L=i.useState,R=i.useEffect,C=i.useLayoutEffect,I=i.useDebugValue;function N(e,t){var o=t(),n=L({inst:{value:o,getSnapshot:t}}),r=n[0].inst,u=n[1];return C(function(){r.value=o,r.getSnapshot=t,m(r)&&u({inst:r})},[e,o,t]),R(function(){return m(r)&&u({inst:r}),e(function(){m(r)&&u({inst:r})})},[e]),I(o),o}function m(e){var t=e.getSnapshot;e=e.value;try{var o=t();return!D(e,o)}catch{return!0}}function O(e,t){return t()}var B=typeof window>"u"||typeof window.document>"u"||typeof window.document.createElement>"u"?O:N;w.useSyncExternalStore=i.useSyncExternalStore!==void 0?i.useSyncExternalStore:B;j.exports=w;var G=j.exports;/**
 * @license React
 * use-sync-external-store-shim/with-selector.production.js
 *
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */var y=k,F=G;function H(e,t){return e===t&&(e!==0||1/e===1/t)||e!==e&&t!==t}var U=typeof Object.is=="function"?Object.is:H,W=F.useSyncExternalStore,A=y.useRef,J=y.useEffect,K=y.useMemo,P=y.useDebugValue;$.useSyncExternalStoreWithSelector=function(e,t,o,n,r){var u=A(null);if(u.current===null){var a={hasValue:!1,value:null};u.current=a}else a=u.current;u=K(function(){function p(c){if(!h){if(h=!0,f=c,c=n(c),r!==void 0&&a.hasValue){var s=a.value;if(r(s,c))return d=s}return d=c}if(s=d,U(f,c))return s;var S=n(c);return r!==void 0&&r(s,S)?(f=c,s):(f=c,d=S)}var h=!1,f,d,E=o===void 0?null:o;return[function(){return p(t())},E===null?void 0:function(){return p(E())}]},[t,o,n,r]);var l=W(e,u[0],u[1]);return J(function(){a.hasValue=!0,a.value=l},[l]),P(l),l};x.exports=$;var Q=x.exports;const te=M(Q);export{Y as B,Z as C,q as E,ee as L,G as s,te as u,Q as w};
