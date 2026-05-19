import{d as S}from"./index-Cibb-3wn.js";import{r as b,g as k}from"./query-Bw69rE6b.js";/**
 * @license lucide-react v0.577.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const x=[["path",{d:"M6 12h9a4 4 0 0 1 0 8H7a1 1 0 0 1-1-1V5a1 1 0 0 1 1-1h7a4 4 0 0 1 0 8",key:"mg9rjx"}]],T=S("bold",x);/**
 * @license lucide-react v0.577.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const V=[["path",{d:"M3 5h.01",key:"18ugdj"}],["path",{d:"M3 12h.01",key:"nlz23k"}],["path",{d:"M3 19h.01",key:"noohij"}],["path",{d:"M8 5h13",key:"1pao27"}],["path",{d:"M8 12h13",key:"1za7za"}],["path",{d:"M8 19h13",key:"m83p4d"}]],X=S("list",V);var j={exports:{}},w={},M={exports:{}},$={};/**
 * @license React
 * use-sync-external-store-shim.production.js
 *
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */var f=b;function _(e,t){return e===t&&(e!==0||1/e===1/t)||e!==e&&t!==t}var g=typeof Object.is=="function"?Object.is:_,z=f.useState,D=f.useEffect,L=f.useLayoutEffect,R=f.useDebugValue;function I(e,t){var u=t(),n=z({inst:{value:u,getSnapshot:t}}),r=n[0].inst,o=n[1];return L(function(){r.value=u,r.getSnapshot=t,h(r)&&o({inst:r})},[e,u,t]),D(function(){return h(r)&&o({inst:r}),e(function(){h(r)&&o({inst:r})})},[e]),R(u),u}function h(e){var t=e.getSnapshot;e=e.value;try{var u=t();return!g(e,u)}catch{return!0}}function O(e,t){return t()}var B=typeof window>"u"||typeof window.document>"u"||typeof window.document.createElement>"u"?O:I;$.useSyncExternalStore=f.useSyncExternalStore!==void 0?f.useSyncExternalStore:B;M.exports=$;var C=M.exports;/**
 * @license React
 * use-sync-external-store-shim/with-selector.production.js
 *
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */var v=b,G=C;function N(e,t){return e===t&&(e!==0||1/e===1/t)||e!==e&&t!==t}var F=typeof Object.is=="function"?Object.is:N,H=G.useSyncExternalStore,U=v.useRef,W=v.useEffect,A=v.useMemo,J=v.useDebugValue;w.useSyncExternalStoreWithSelector=function(e,t,u,n,r){var o=U(null);if(o.current===null){var c={hasValue:!1,value:null};o.current=c}else c=o.current;o=A(function(){function m(a){if(!p){if(p=!0,l=a,a=n(a),r!==void 0&&c.hasValue){var s=c.value;if(r(s,a))return d=s}return d=a}if(s=d,F(l,a))return s;var E=n(a);return r!==void 0&&r(s,E)?(l=a,s):(l=a,d=E)}var p=!1,l,d,y=u===void 0?null:u;return[function(){return m(t())},y===null?void 0:function(){return m(y())}]},[t,u,n,r]);var i=H(e,o[0],o[1]);return W(function(){c.hasValue=!0,c.value=i},[i]),J(i),i};j.exports=w;var K=j.exports;const Y=k(K);export{T as B,X as L,C as s,Y as u,K as w};
