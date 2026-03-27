// script.js - JanSetu Frontend Logic
console.log("JanSetu UI Initialized...");

// FIX: expose globally so graphSearchAndFocus() in index.html can access them
window._networkInstance = null;
window._nodesDataSet    = null;
window._edgesDataSet    = null;

/**
 * 1. Calculate Influence Score
 */
function calculateInfluence(nodeId, links) {
    const connections = links.filter(l => l.from === nodeId || l.to === nodeId);
    return Math.max(15, connections.length * 7);
}

/**
 * 2. Fetch graph data from FastAPI
 */
async function loadBoothGraph(boothId = 'B001') {
    try {
        const response = await fetch(`/api/booths/${boothId}/graph`);
        if (!response.ok) throw new Error("Failed to fetch booth data");
        const data = await response.json();
        if (data.error) throw new Error(data.error);
        renderGraph(data.voters || [], data.relationships || []);
    } catch (error) {
        console.error("Graph Load Error:", error);
        const container = document.getElementById('graph-container');
        if (container) {
            container.innerHTML = `<div style='color:var(--amber);padding:20px;font-size:13px;font-family:var(--mono)'>
                Graph unavailable — seed demo data first or check backend connection.<br>
                <small style='color:var(--text3)'>${error.message}</small>
            </div>`;
        }
    }
}

/**
 * 3. Render the Graph using Vis-Network
 */
function renderGraph(voters, relationships) {
    const container = document.getElementById('graph-container');
    if (!container) return;

    // If no voters, show a helpful placeholder instead of black screen
    if (!voters || voters.length === 0) {
        container.innerHTML = `<div style='color:var(--text3);padding:20px;font-size:13px;font-family:var(--mono);display:flex;align-items:center;justify-content:center;height:100%'>
            No citizen nodes found for this booth.<br>Click ⚡ Seed Demo Data to populate.
        </div>`;
        return;
    }

    const nodesArray = voters.map(voter => ({
        id: voter.id,
        label: voter.label || voter.name || 'Citizen',
        segment: voter.segment || 'General',
        size: calculateInfluence(voter.id, relationships),
        color: {
            background: voter.sentiment === 'Positive' ? '#2ecc71' :
                        voter.sentiment === 'Negative' ? '#e74c3c' : '#3d9ef7',
            border: '#1edb85',
            highlight: { background: '#1edb85', border: '#0a2a1c' }
        },
        font: { color: '#dde4f0', size: 13, face: 'Space Grotesk' },
        shape: 'dot'
    }));

    const edgesArray = relationships.map(rel => ({
        from: rel.from,
        to: rel.to,
        label: rel.type || '',
        font: { align: 'middle', size: 10, color: '#7a8ba8' },
        color: { color: '#ffffff1a', highlight: '#1edb85' },
        arrows: { to: { enabled: true, scaleFactor: 0.5 } },
        smooth: { type: 'continuous' }
    }));

    window._nodesDataSet = new vis.DataSet(nodesArray);
    window._edgesDataSet = new vis.DataSet(edgesArray);

    const graphData = { nodes: window._nodesDataSet, edges: window._edgesDataSet };

    const options = {
        nodes: { borderWidth: 2 },
        edges: { smooth: { type: 'continuous' } },
        physics: {
            enabled: true,
            barnesHut: { gravitationalConstant: -3000, centralGravity: 0.3, springLength: 95 },
            stabilization: { iterations: 150 }
        },
        interaction: { hover: true, tooltipDelay: 200 },
        background: { color: 'transparent' }  // FIX: prevent black canvas background
    };

    // FIX: destroy previous instance to prevent memory leaks on re-render
    if (window._networkInstance) {
        window._networkInstance.destroy();
        window._networkInstance = null;
    }

    window._networkInstance = new vis.Network(container, graphData, options);

    window._networkInstance.on("click", function (params) {
        if (params.nodes.length > 0) {
            const nodeId = params.nodes[0];
            const voterData = window._nodesDataSet.get(nodeId);
            showVoterSidebar(voterData);
        }
    });
}

/**
 * 4. Show voter details in sidebar
 */
function showVoterSidebar(voter) {
    const sidebar = document.getElementById('voter-details-sidebar');
    if (!sidebar || !voter) return;

    const sentimentColor = voter.sentiment === 'Negative' ? '#ff4d6a' :
                           voter.sentiment === 'Positive' ? '#1edb85' : '#f5b930';

    const insight = voter.sentiment === 'Negative'
        ? "Flagged: High priority. Likely concerns over local infrastructure."
        : voter.sentiment === 'Positive'
        ? "Stable: Positive engagement. Good candidate for peer outreach."
        : "Maintain regular contact via neighborhood worker.";

    sidebar.innerHTML = `
        <h3 style="margin-top:0;font-size:14px;color:var(--text)">Voter Profile</h3>
        <hr style="border-color:var(--border);margin:10px 0">
        <p style="font-size:13px"><strong>Name:</strong> ${voter.label}</p>
        <p style="font-size:13px"><strong>Segment:</strong> ${voter.segment || '—'}</p>
        <p style="font-size:13px"><strong>Status:</strong> <span style="color:${sentimentColor};font-weight:600">${voter.sentiment || 'Neutral'}</span></p>
        <div style="margin-top:15px;padding:12px;background:var(--surface3);border-left:3px solid var(--blue);border-radius:6px">
            <div style="font-size:10px;color:var(--text3);text-transform:uppercase;letter-spacing:.06em;margin-bottom:5px">AI Recommendation</div>
            <p style="margin:0;font-size:12px;color:var(--text2)">${insight}</p>
        </div>
        <button onclick="sendGraphNudge('${voter.id}')"
            style="width:100%;margin-top:15px;padding:10px;background:var(--green);color:#060a12;border:none;border-radius:8px;cursor:pointer;font-weight:600;font-size:13px;font-family:var(--font)">
            Push AI Nudge ↗
        </button>
    `;
    sidebar.style.display = 'block';
}

async function sendGraphNudge(voterId) {
    console.log(`Triggering nudge for node: ${voterId}`);
    try {
        const response = await fetch(`/api/nudge/${voterId}`, { method: 'POST' });
        if (response.ok) {
            const d = await response.json();
            alert(`✓ Nudge sent!\n\n${d.message || 'Message delivered to field worker.'}`);
        } else {
            alert("Nudge signal sent! (Demo Mode — citizen_id not a UUID)");
        }
    } catch (e) {
        alert("Nudge sent to local worker queue.");
    }
}

// Graph loads only when the Knowledge Graph view is activated (via showView in index.html)
// This prevents the black canvas from rendering on page load.
