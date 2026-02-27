/**
 * Process imported JSON data for the web app.
 *
 * Reads a merged/imported graphology JSON file, runs ForceAtlas2 + Louvain,
 * and saves pako-compressed output with the filename the web app expects.
 *
 * Usage:
 *   node bin/process_imported_data.js <input.json> <entity> <size> [<links_type>]
 *
 * Arguments:
 *   input      - Path to imported graphology JSON (e.g. data/emreparker_creators.json)
 *   entity     - "creators" or "characters"
 *   size       - "full" or "small"
 *   links_type - "stories" (default) or "comics" — node attribute to use for weights/sizes
 *
 * Output:
 *   data/Marvel_<entity>_by_<links_type>_<size>.json.gz
 *
 * Examples:
 *   node bin/process_imported_data.js data/Marvel_creators_merged.json creators full
 *   node bin/process_imported_data.js data/Marvel_characters_merged.json characters full
 */

const fs = require('fs');
const path = require('path');
const pako = require('pako');
const graphology = require('graphology');
const layouts = require('graphology-layout');
const forceAtlas2 = require('graphology-layout-forceatlas2');
const noverlap = require('graphology-layout-noverlap');
const louvain = require('graphology-communities-louvain');

const args = process.argv.slice(2);
if (args.length < 3) {
  console.error('Usage: node bin/process_imported_data.js <input.json> <entity> <size> [<links_type>]');
  console.error('  entity:     creators | characters');
  console.error('  size:       full | small');
  console.error('  links_type: stories (default) | comics');
  process.exit(1);
}

const inputFile  = args[0];
const entity     = args[1];   // "creators" or "characters"
const size       = args[2];   // "full" or "small"
const linksType  = args[3] || 'stories';

const FA2Iterations    = 15000;
const batchIterations  = 1000;

// Output filename matches what index.ts loads:
// fetch("./data/Marvel_" + ent + "_by_stories_full" + ".json.gz")
const outputFile = path.join('data', `Marvel_${entity}_by_${linksType}_${size}.json.gz`);

// ─── Helpers ─────────────────────────────────────────────────────────────────

function loadJSON(filename) {
  console.log('Reading', filename, '...');
  const raw = fs.readFileSync(filename, { flag: 'r' });
  // Support both plain JSON and pako-compressed JSON
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    parsed = JSON.parse(pako.inflate(raw, { to: 'string' }));
  }
  // Strip self-loops (present as bugs in some source data)
  const before = parsed.edges ? parsed.edges.length : 0;
  parsed.edges = (parsed.edges || []).filter(e => e.source !== e.target);
  const removed = before - parsed.edges.length;
  if (removed > 0) console.log(`  Stripped ${removed} self-loop edge(s)`);
  return graphology.Graph.from(parsed);
}

function savePako(graph, filename) {
  console.log('Writing', filename, '...');
  fs.writeFileSync(filename, pako.deflate(JSON.stringify(graph)));
  const kb = Math.round(fs.statSync(filename).size / 1024);
  console.log(' -> Saved', filename, `(${kb} KB)`);
}

function runBatchFA2(graph, settings, done, total, callback) {
  const t0 = Date.now();
  forceAtlas2.assign(graph, {
    iterations: batchIterations,
    getEdgeWeight: 'weight',
    settings: settings
  });
  done += batchIterations;
  console.log(` FA2 ${done}/${total} (${((Date.now()-t0)/1000).toFixed(1)}s)`);
  if (done < total) runBatchFA2(graph, settings, done, total, callback);
  else callback();
}

// ─── Main ────────────────────────────────────────────────────────────────────

const graph = loadJSON(inputFile);

console.log('Nodes:', graph.order);
console.log('Edges:', graph.size);
console.log('Entity:', entity, '| Size:', size, '| Links type:', linksType);

// 1. Circular starting positions + node sizes
const circularPositions = layouts.circular(graph, { scale: 50 });
graph.forEachNode((node, attrs) => {
  const linkCount = attrs[linksType] || 1;
  graph.mergeNodeAttributes(node, {
    x: circularPositions[node].x,
    y: circularPositions[node].y,
    size: Math.pow(linkCount, 0.2)
      * (entity === 'characters' ? 1.75 : 1.25)
      * (size === 'small' ? 1.75 : 1.25)
  });
});

// 2. PMI edge weights (same as spatialize-network.js)
const total = graph.reduceNodes((sum, node, attrs) => sum + (attrs[linksType] || 0), 0);
graph.forEachEdge((edge, { weight }, n1, n2, n1a, n2a) => {
  const n1count = n1a[linksType] || 1;
  const n2count = n2a[linksType] || 1;
  const deg1 = graph.degree(n1);
  const deg2 = graph.degree(n2);
  const pmi = Math.log(total * weight / (n1count * n2count));
  graph.setEdgeAttribute(edge, 'weight',
    Math.max(deg1 === 1 || deg2 === 1 ? 1 : 0, pmi)
  );
});

// 3. Louvain community detection (characters only)
if (entity === 'characters') {
  console.log('Running Louvain community detection...');
  louvain.assign(graph, { resolution: 1.2 });
}

// 4. ForceAtlas2
console.log(`Starting ForceAtlas2 for ${FA2Iterations} iterations...`);
const settings = forceAtlas2.inferSettings(graph);
settings.edgeWeightInfluence = 0.5;
const t0 = Date.now();

runBatchFA2(graph, settings, 0, FA2Iterations, () => {
  console.log(`FA2 done in ${((Date.now()-t0)/1000).toFixed(1)}s`);

  // 5. Noverlap (prevent node overlap)
  noverlap.assign(graph);

  // 6. Quantise edge weights to integers (reduces file size)
  graph.forEachEdge((edge, { weight }) =>
    graph.setEdgeAttribute(edge, 'weight', Math.round(1000 * weight))
  );

  // 7. Save
  savePako(graph, outputFile);
  console.log('\nDone! Load in web app as: data/Marvel_' + entity + '_by_' + linksType + '_' + size + '.json.gz');
});
