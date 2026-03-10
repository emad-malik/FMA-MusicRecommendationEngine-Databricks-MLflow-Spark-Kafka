// ===== Global State =====
let allTracks = [];
let favorites = [];

// ===== Toast Notifications =====
function showToast(message, type = 'success') {
    const toast = document.getElementById('toast');
    const toastMessage = document.getElementById('toast-message');
    
    toastMessage.textContent = message;
    toast.className = `toast show ${type}`;
    
    setTimeout(() => {
        toast.classList.remove('show');
    }, 3000);
}

// ===== Load Tracks from API =====
async function loadTracks() {
    const loading = document.getElementById('loading');
    const tracksContainer = document.getElementById('tracks-container');
    const noTracks = document.getElementById('no-tracks');
    const tracksCount = document.getElementById('tracks-count');
    
    loading.style.display = 'block';
    tracksContainer.style.display = 'none';
    noTracks.style.display = 'none';
    
    try {
        const response = await fetch('/api/tracks');
        const data = await response.json();
        
        if (data.success) {
            allTracks = data.tracks;
            tracksCount.textContent = data.count;
            renderTracks(allTracks);
            
            if (allTracks.length === 0) {
                loading.style.display = 'none';
                noTracks.style.display = 'block';
            } else {
                loading.style.display = 'none';
                tracksContainer.style.display = 'grid';
            }
        } else {
            throw new Error(data.error);
        }
    } catch (error) {
        loading.style.display = 'none';
        noTracks.style.display = 'block';
        showToast('Error loading tracks: ' + error.message, 'error');
    }
}

// ===== Render Tracks =====
function renderTracks(tracks) {
    const tracksContainer = document.getElementById('tracks-container');
    tracksContainer.innerHTML = '';
    
    tracks.forEach(track => {
        const trackCard = createTrackCard(track);
        tracksContainer.appendChild(trackCard);
    });
}

// ===== Create Track Card Element =====
function createTrackCard(track) {
    const card = document.createElement('div');
    card.className = 'track-card';
    card.dataset.trackId = track.track_id;
    
    const isFavorite = favorites.includes(track.track_id);
    const clusterClass = track.cluster_id !== null && track.cluster_id !== undefined 
        ? `cluster-${track.cluster_id}` 
        : 'cluster-unknown';
    
    card.innerHTML = `
        <div class="track-header">
            <div class="track-icon">
                <i class="fas fa-music"></i>
            </div>
            <button class="track-favorite ${isFavorite ? 'favorited' : ''}" 
                    onclick="toggleFavorite('${track.track_id}', event)">
                <i class="fas fa-heart"></i>
            </button>
        </div>
        <div class="track-info">
            <h3 title="${track.filename}">${track.filename}</h3>
            <div class="track-meta">
                <div class="track-meta-item">
                    <i class="fas fa-fingerprint"></i>
                    <span>ID: ${track.track_id}</span>
                </div>
                ${track.cluster_id !== null && track.cluster_id !== undefined ? `
                <div class="track-meta-item">
                    <span class="cluster-badge ${clusterClass}">
                        <i class="fas fa-layer-group"></i>
                        Cluster ${track.cluster_id}
                    </span>
                </div>
                ` : ''}
            </div>
        </div>
        <div class="track-actions">
            <button class="btn btn-primary btn-small" onclick="playTrack('${track.track_id}', '${track.filename}', '${track.url}', ${track.cluster_id})">
                <i class="fas fa-play"></i> Play
            </button>
            ${track.cluster_id !== null && track.cluster_id !== undefined ? `
            <button class="btn btn-secondary btn-small" onclick="showRecommendations('${track.track_id}')">
                <i class="fas fa-list-ul"></i> Similar
            </button>
            ` : ''}
        </div>
    `;
    
    return card;
}

// ===== Filter Tracks =====
function filterTracks() {
    const searchInput = document.getElementById('search-input').value.toLowerCase();
    const clusterFilter = document.getElementById('cluster-filter').value;
    
    let filteredTracks = allTracks;
    
    // Filter by search term
    if (searchInput) {
        filteredTracks = filteredTracks.filter(track => 
            track.filename.toLowerCase().includes(searchInput) ||
            track.track_id.toString().includes(searchInput)
        );
    }
    
    // Filter by cluster
    if (clusterFilter !== 'all') {
        const clusterId = parseInt(clusterFilter);
        filteredTracks = filteredTracks.filter(track => track.cluster_id === clusterId);
    }
    
    renderTracks(filteredTracks);
    
    const noTracks = document.getElementById('no-tracks');
    const tracksContainer = document.getElementById('tracks-container');
    
    if (filteredTracks.length === 0) {
        tracksContainer.style.display = 'none';
        noTracks.style.display = 'block';
    } else {
        tracksContainer.style.display = 'grid';
        noTracks.style.display = 'none';
    }
}

// ===== Play Track =====
function playTrack(trackId, filename, url, clusterId) {
    const modal = document.getElementById('player-modal');
    const audioPlayer = document.getElementById('audio-player');
    const trackName = document.getElementById('player-track-name');
    const trackIdSpan = document.getElementById('player-track-id');
    const clusterIdSpan = document.getElementById('player-cluster-id');
    
    trackName.textContent = filename;
    trackIdSpan.textContent = trackId;
    clusterIdSpan.textContent = clusterId !== null && clusterId !== undefined ? clusterId : 'N/A';
    
    audioPlayer.src = url;
    audioPlayer.load();
    
    modal.style.display = 'block';
}

function closePlayer() {
    const modal = document.getElementById('player-modal');
    const audioPlayer = document.getElementById('audio-player');
    
    audioPlayer.pause();
    audioPlayer.src = '';
    modal.style.display = 'none';
}

// ===== Recommendations =====
async function showRecommendations(trackId) {
    const modal = document.getElementById('recommendations-modal');
    const content = document.getElementById('recommendations-content');
    
    content.innerHTML = '<div class="loading"><div class="spinner"></div><p>Loading recommendations...</p></div>';
    modal.style.display = 'block';
    
    try {
        const response = await fetch(`/api/recommendations/${trackId}?limit=10`);
        const data = await response.json();
        
        if (data.success) {
            if (data.recommendations.length === 0) {
                content.innerHTML = `
                    <div class="empty-state">
                        <i class="fas fa-music"></i>
                        <p>No similar tracks found in the same cluster</p>
                    </div>
                `;
            } else {
                content.innerHTML = `
                    <p style="margin-bottom: 1rem; color: var(--gray);">
                        Found ${data.count} similar tracks in Cluster ${data.cluster_id}
                    </p>
                    <div class="tracks-grid">
                        ${data.recommendations.map(track => createTrackCardHTML(track)).join('')}
                    </div>
                `;
                
                // Reattach event listeners
                attachTrackCardListeners();
            }
        } else {
            throw new Error(data.error);
        }
    } catch (error) {
        content.innerHTML = `
            <div class="alert alert-error">
                <i class="fas fa-exclamation-circle"></i>
                <div>Error loading recommendations: ${error.message}</div>
            </div>
        `;
    }
}

function createTrackCardHTML(track) {
    const isFavorite = favorites.includes(track.track_id);
    const clusterClass = track.cluster_id !== null && track.cluster_id !== undefined 
        ? `cluster-${track.cluster_id}` 
        : 'cluster-unknown';
    
    return `
        <div class="track-card" data-track-id="${track.track_id}">
            <div class="track-header">
                <div class="track-icon">
                    <i class="fas fa-music"></i>
                </div>
                <button class="track-favorite ${isFavorite ? 'favorited' : ''}" 
                        onclick="toggleFavorite('${track.track_id}', event)">
                    <i class="fas fa-heart"></i>
                </button>
            </div>
            <div class="track-info">
                <h3 title="${track.filename}">${track.filename}</h3>
                <div class="track-meta">
                    <div class="track-meta-item">
                        <i class="fas fa-fingerprint"></i>
                        <span>ID: ${track.track_id}</span>
                    </div>
                    ${track.cluster_id !== null && track.cluster_id !== undefined ? `
                    <div class="track-meta-item">
                        <span class="cluster-badge ${clusterClass}">
                            <i class="fas fa-layer-group"></i>
                            Cluster ${track.cluster_id}
                        </span>
                    </div>
                    ` : ''}
                </div>
            </div>
            <div class="track-actions">
                <button class="btn btn-primary btn-small" onclick="playTrack('${track.track_id}', '${track.filename}', '${track.url}', ${track.cluster_id})">
                    <i class="fas fa-play"></i> Play
                </button>
            </div>
        </div>
    `;
}

function attachTrackCardListeners() {
    // Event listeners are handled via onclick in HTML
}

function closeRecommendations() {
    const modal = document.getElementById('recommendations-modal');
    modal.style.display = 'none';
}

// ===== Favorites Management =====
async function loadFavoritesCount() {
    try {
        const response = await fetch('/api/favorites');
        const data = await response.json();
        
        if (data.success) {
            favorites = data.favorites;
            updateFavoritesCount();
        }
    } catch (error) {
        console.error('Error loading favorites:', error);
    }
}

function updateFavoritesCount() {
    const favoritesCount = document.getElementById('favorites-count');
    favoritesCount.textContent = favorites.length;
}

async function toggleFavorite(trackId, event) {
    event.stopPropagation();
    
    const isFavorite = favorites.includes(trackId);
    
    try {
        if (isFavorite) {
            // Remove from favorites
            const response = await fetch(`/api/favorites?track_id=${trackId}`, {
                method: 'DELETE'
            });
            const data = await response.json();
            
            if (data.success) {
                favorites = data.favorites;
                showToast('Removed from favorites');
            }
        } else {
            // Add to favorites
            const response = await fetch('/api/favorites', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ track_id: trackId })
            });
            const data = await response.json();
            
            if (data.success) {
                favorites = data.favorites;
                showToast('Added to favorites');
            }
        }
        
        // Update UI
        updateFavoritesCount();
        updateFavoriteButtons(trackId, !isFavorite);
        
    } catch (error) {
        showToast('Error updating favorites', 'error');
    }
}

function updateFavoriteButtons(trackId, isFavorite) {
    const buttons = document.querySelectorAll(`[onclick*="toggleFavorite('${trackId}"`);
    buttons.forEach(btn => {
        if (isFavorite) {
            btn.classList.add('favorited');
        } else {
            btn.classList.remove('favorited');
        }
    });
}

async function showFavorites() {
    const modal = document.getElementById('favorites-modal');
    const content = document.getElementById('favorites-content');
    
    modal.style.display = 'block';
    
    if (favorites.length === 0) {
        content.innerHTML = `
            <div class="empty-state">
                <i class="fas fa-heart"></i>
                <p>No favorite tracks yet</p>
                <p style="font-size: 0.875rem; margin-top: 0.5rem;">
                    Click the heart icon on any track to add it to your favorites
                </p>
            </div>
        `;
        return;
    }
    
    const favoriteTracks = allTracks.filter(track => favorites.includes(track.track_id));
    
    content.innerHTML = `
        <div class="tracks-grid">
            ${favoriteTracks.map(track => createTrackCardHTML(track)).join('')}
        </div>
    `;
    
    attachTrackCardListeners();
}

function closeFavorites() {
    const modal = document.getElementById('favorites-modal');
    modal.style.display = 'none';
}

// ===== Cluster Statistics =====
async function showStats() {
    const modal = document.getElementById('stats-modal');
    const content = document.getElementById('stats-content');
    
    content.innerHTML = '<div class="loading"><div class="spinner"></div><p>Loading statistics...</p></div>';
    modal.style.display = 'block';
    
    try {
        const response = await fetch('/api/cluster-stats');
        const data = await response.json();
        
        if (data.success) {
            const clusterStats = data.cache_stats;
            const totalTracks = data.total_tracks;
            
            let statsHTML = `
                <div style="margin-bottom: 1.5rem;">
                    <h3 style="margin-bottom: 1rem;">Track Distribution</h3>
                    <p style="font-size: 1.1rem; color: var(--gray);">
                        Total Tracks: <strong>${totalTracks}</strong>
                    </p>
                </div>
            `;
            
            if (Object.keys(clusterStats).length > 0) {
                statsHTML += '<div style="display: grid; gap: 1rem;">';
                
                for (const [clusterId, count] of Object.entries(clusterStats)) {
                    const percentage = ((count / totalTracks) * 100).toFixed(1);
                    statsHTML += `
                        <div class="card" style="margin: 0;">
                            <div class="card-body" style="padding: 1.5rem;">
                                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                                    <h4 style="margin: 0;">
                                        <span class="cluster-badge cluster-${clusterId}">
                                            <i class="fas fa-layer-group"></i>
                                            Cluster ${clusterId}
                                        </span>
                                    </h4>
                                    <span style="font-size: 1.5rem; font-weight: 700; color: var(--primary-color);">
                                        ${count}
                                    </span>
                                </div>
                                <div style="background: var(--gray-light); height: 8px; border-radius: 4px; overflow: hidden;">
                                    <div style="width: ${percentage}%; height: 100%; background: var(--primary-color);"></div>
                                </div>
                                <p style="margin-top: 0.5rem; font-size: 0.875rem; color: var(--gray);">
                                    ${percentage}% of total tracks
                                </p>
                            </div>
                        </div>
                    `;
                }
                
                statsHTML += '</div>';
            } else {
                statsHTML += '<p style="color: var(--gray);">No cluster statistics available</p>';
            }
            
            content.innerHTML = statsHTML;
        } else {
            throw new Error(data.error);
        }
    } catch (error) {
        content.innerHTML = `
            <div class="alert alert-error">
                <i class="fas fa-exclamation-circle"></i>
                <div>Error loading statistics: ${error.message}</div>
            </div>
        `;
    }
}

function closeStats() {
    const modal = document.getElementById('stats-modal');
    modal.style.display = 'none';
}

// ===== Modal Close on Outside Click =====
window.onclick = function(event) {
    const modals = document.querySelectorAll('.modal');
    modals.forEach(modal => {
        if (event.target === modal) {
            modal.style.display = 'none';
            
            // Stop audio if player modal is closed
            if (modal.id === 'player-modal') {
                const audioPlayer = document.getElementById('audio-player');
                audioPlayer.pause();
                audioPlayer.src = '';
            }
        }
    });
}

// ===== Keyboard Shortcuts =====
document.addEventListener('keydown', function(event) {
    // ESC key to close modals
    if (event.key === 'Escape') {
        const modals = document.querySelectorAll('.modal');
        modals.forEach(modal => {
            if (modal.style.display === 'block') {
                modal.style.display = 'none';
                
                // Stop audio if player modal is closed
                if (modal.id === 'player-modal') {
                    const audioPlayer = document.getElementById('audio-player');
                    audioPlayer.pause();
                    audioPlayer.src = '';
                }
            }
        });
    }
});
