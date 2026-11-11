import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {makeServer} from './server.js';
test('本地协议提供约定响应并记录请求', async () => {
  const scenario=JSON.parse(await readFile(new URL('./scenario.json',import.meta.url),'utf8'));
  const server=makeServer(scenario); await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  try {
    const base='http://127.0.0.1:'+server.address().port;
    for (const route of scenario.routes) {
      const url=new URL(route.path,base); for (const [key,value] of Object.entries(route.query??{})) url.searchParams.set(key,value);
      for (const expected of route.responses) {
        const response=await fetch(url,{method:route.method??'GET'});
        assert.equal(response.status,expected.status??200);
        if (expected.json!==undefined) assert.deepEqual(await response.json(),expected.json);
        else assert.equal(await response.text(),expected.body??'');
      }
    }
    assert.ok((await (await fetch(base+'/_requests')).json()).length>0);
  } finally {server.closeAllConnections(); await new Promise(resolve=>server.close(resolve));}
});
