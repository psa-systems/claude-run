/**
*
 * Read the Quick Start Guide:
 * https://www.jetbrains.com/help/youtrack/devportal/Quick-Start-Guide-Workflows-JS.html
 */
const entities = require('@jetbrains/youtrack-scripting-api/entities');

// Project key + number, e.g. A8N-123. Tweak charset if your key allows digits/underscores.
const ID = '[A-Za-z][A-Za-z0-9]*-\\d+';

function collect(re, text, group, out) {
  let m;
  while ((m = re.exec(text)) !== null) { out.push(m[group]); }
}

// Ids a PR claims to own: title tokens, "#KEY-N" trailer lines, "Closes/Fixes/Resolves [#]KEY-N".
function ownedIds(pr) {
  const title = pr.title || '';
  const text = pr.text || '';
  const ids = [];
  collect(new RegExp(ID, 'g'), title, 0, ids);
  collect(new RegExp('^\\s*#(' + ID + ')\\s*$', 'gm'), text, 1, ids);
  collect(new RegExp('\\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\\s*:?\\s*#?(' + ID + ')', 'gi'), text, 1, ids);
  return ids;
}

exports.rule = entities.Issue.onChange({
  title: 'Resolve issue when its own PR is merged',
  // Fire when a PR is attached/changed this transaction. Manual edits have empty added set.
  guard: (ctx) => ctx.issue.pullRequests.added.isNotEmpty(),
  action: (ctx) => {
    const issue = ctx.issue;
    const myId = issue.id; // expect readable id like 'A8N-123'; CONFIRM in log below

    let merged = false;

    issue.pullRequests.forEach((pr) => {
      const justMerged = pr.state && pr.state.name === 'MERGED' &&
                         (!pr.previousState || pr.previousState.name !== 'MERGED');

      const branch = pr.branch || '';
      const ids = ownedIds(pr);
      const owns = ids.indexOf(myId) !== -1;

      // Diagnostic: confirm id format, branch population, source-vs-target, state literals.
      console.log('myId=' + myId +
                  ' branch=' + branch +
                  ' owned=' + JSON.stringify(ids) +
                  ' owns=' + owns +
                  ' state=' + (pr.state && pr.state.name) +
                  ' prev=' + (pr.previousState && pr.previousState.name));
      console.log('pr.title=' + pr.title);
      //console.log('pr.text=' + pr.text);

      if (justMerged && owns) { merged = true; }
    });
    
    if (!merged) { return; }
    if (!issue.fields.is(ctx.State, ctx.State.Fixed)) {
      issue.fields.State = ctx.State.Fixed;
    }

  },
  requirements: {
    State: { type: entities.State.fieldType, Fixed: { name: 'Done' } }
  }
});
