import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {pathToFileURL} from 'node:url';
export function makeServer(scenario) {
  const counts = new Map(); const requests = [];
  return http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    const chunks=[]; for await (const part of req) chunks.push(part);
    requests.push({method:req.method,path:req.url,body:Buffer.concat(chunks).toString(),group:req.headers['x-credential-group']});
    const send=(status,headers,body)=>{res.writeHead(status,{'Content-Length':Buffer.byteLength(body),...headers});res.end(body);};
    if (url.pathname === '/_requests') return send(200,{'Content-Type':'application/json'},JSON.stringify(requests));
    const candidates=scenario.routes.filter(route => route.path===url.pathname && (route.method??'GET')===req.method && Object.entries(route.query??{}).every(([key,value])=>url.searchParams.get(key)===String(value)));
    candidates.sort((a,b)=>Object.keys(b.query??{}).length-Object.keys(a.query??{}).length);
    const route=candidates[0];
    if (!route) return send(404,{},'unknown local route');
    const key=scenario.routes.indexOf(route); const count=counts.get(key)??0; counts.set(key,count+1);
    const reply=route.responses[Math.min(count,route.responses.length-1)];
    const body=reply.json!==undefined ? JSON.stringify(reply.json) : reply.body??'';
    send(reply.status??200,{...(reply.json!==undefined?{'Content-Type':'application/json'}:{}),...reply.headers},body);
  });
}
if (process.argv[1] && import.meta.url===pathToFileURL(process.argv[1]).href) {
  const scenario=JSON.parse(await readFile(new URL('./scenario.json',import.meta.url),'utf8'));
  const port=Number(process.env.PORT??8765); const server=makeServer(scenario);
  server.listen(port,'127.0.0.1',()=>process.stdout.write(JSON.stringify({url:'http://127.0.0.1:'+server.address().port})+'\n'));
}
