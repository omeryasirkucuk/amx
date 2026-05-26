import{d as h}from"./index-CMwgutx7.js";import{r as k,g as b}from"./query-Bw69rE6b.js";/**
 * @license lucide-react v0.577.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const M=[["path",{d:"M6 12h9a4 4 0 0 1 0 8H7a1 1 0 0 1-1-1V5a1 1 0 0 1 1-1h7a4 4 0 0 1 0 8",key:"mg9rjx"}]],X=h("bold",M);/**
 * @license lucide-react v0.577.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const _=[["circle",{cx:"12",cy:"12",r:"1",key:"41hilf"}],["circle",{cx:"19",cy:"12",r:"1",key:"1wjl8i"}],["circle",{cx:"5",cy:"12",r:"1",key:"1pcz8c"}]],Y=h("ellipsis",_);/**
 * @license lucide-react v0.577.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const V=[["path",{d:"M3 5h.01",key:"18ugdj"}],["path",{d:"M3 12h.01",key:"nlz23k"}],["path",{d:"M3 19h.01",key:"noohij"}],["path",{d:"M8 5h13",key:"1pao27"}],["path",{d:"M8 12h13",key:"1za7za"}],["path",{d:"M8 19h13",key:"m83p4d"}]],Z=h("list",V);var j={exports:{}},w={},x={exports:{}},$={};/**
 * @license React
 * use-sync-external-store-shim.production.js
 *
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */var i=k;function z(e,t){return e===t&&(e!==0||1/e===1/t)||e!==e&&t!==t}var g=typeof Object.is=="function"?Object.is:z,D=i.useState,L=i.useEffect,R=i.useLayoutEffect,I=i.useDebugValue;function O(e,t){var u=t(),a=D({inst:{value:u,getSnapshot:t}}),r=a[0].inst,o=a[1];return R(function(){r.value=u,r.getSnapshot=t,p(r)&&o({inst:r})},[e,u,t]),L(function(){return p(r)&&o({inst:r}),e(function(){p(r)&&o({inst:r})})},[e]),I(u),u}function p(e){var t=e.getSnapshot;e=e.value;try{var u=t();return!g(e,u)}catch{return!0}}function N(e,t){return t()}var B=typeof window>"u"||typeof window.document>"u"||typeof window.document.createElement>"u"?N:O;$.useSyncExternalStore=i.useSyncExternalStore!==void 0?i.useSyncExternalStore:B;x.exports=$;var C=x.exports;/**
 * @license React
 * use-sync-external-store-shim/with-selector.production.js
 *
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */var v=k,G=C;function F(e,t){return e===t&&(e!==0||1/e===1/t)||e!==e&&t!==t}var H=typeof Object.is=="function"?Object.is:F,U=G.useSyncExternalStore,W=v.useRef,A=v.useEffect,J=v.useMemo,K=v.useDebugValue;w.useSyncExternalStoreWithSelector=function(e,t,u,a,r){var o=W(null);if(o.current===null){var c={hasValue:!1,value:null};o.current=c}else c=o.current;o=J(function(){function y(n){if(!m){if(m=!0,l=n,n=a(n),r!==void 0&&c.hasValue){var s=c.value;if(r(s,n))return d=s}return d=n}if(s=d,H(l,n))return s;var S=a(n);return r!==void 0&&r(s,S)?(l=n,s):(l=n,d=S)}var m=!1,l,d,E=u===void 0?null:u;return[function(){return y(t())},E===null?void 0:function(){return y(E())}]},[t,u,a,r]);var f=U(e,o[0],o[1]);return A(function(){c.hasValue=!0,c.value=f},[f]),K(f),f};j.exports=w;var P=j.exports;const q=b(P);export{X as B,Y as E,Z as L,C as s,q as u,P as w};
