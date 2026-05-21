//! Integration tests for `WatchFilter`'s directory-watch and event-emit predicates.
//!
//! The directory-watch gate (registration-time pruning) is only consulted by
//! the inotify backend today, so these tests are Linux/Android only.

#![cfg(any(target_os = "linux", target_os = "android"))]

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Result, WatchFilter, Watcher};

/// Drains the receiver for up to `timeout`, collecting every successful event.
fn collect_events(rx: &std::sync::mpsc::Receiver<Result<Event>>, timeout: Duration) -> Vec<Event> {
    let deadline = std::time::Instant::now() + timeout;
    let mut events = Vec::new();
    while let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) {
        match rx.recv_timeout(remaining) {
            Ok(Ok(event)) => events.push(event),
            Ok(Err(_)) => {}
            Err(_) => break,
        }
    }
    events
}

fn paths_under(events: &[Event], root: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<_> = events
        .iter()
        .flat_map(|event| event.paths.iter())
        .filter(|path| path.starts_with(root))
        .cloned()
        .collect();
    paths.sort();
    paths.dedup();
    paths
}

/// Builds a fake `.git/` directory layout under `root` containing both
/// allowlisted (`HEAD`) and excluded (`objects/blob`) files.
fn build_fake_repo(root: &Path) -> std::io::Result<()> {
    let git_dir = root.join(".git");
    fs::create_dir_all(git_dir.join("refs").join("heads"))?;
    fs::create_dir_all(git_dir.join("objects").join("ab"))?;
    fs::write(git_dir.join("HEAD"), b"ref: refs/heads/main\n")?;
    fs::write(
        git_dir.join("objects").join("ab").join("blob"),
        b"initial\n",
    )?;
    fs::write(root.join("file.txt"), b"hello\n")?;
    Ok(())
}

/// Marks `.git/objects/` as a directory that should not be watched so
/// walkdir prunes the subtree at registration time. Verifies:
///
/// * Modifying `.git/HEAD` produces an event (we registered watches along
///   the path needed to reach it).
/// * Modifying `.git/objects/<blob>` does NOT produce an event (the
///   subtree was pruned before any watch was registered).
/// * Modifying a regular file in the repo root still produces an event.
#[test]
fn should_watch_directory_prunes_git_objects() -> std::result::Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    build_fake_repo(dir.path())?;

    let (tx, rx) = std::sync::mpsc::channel();
    let mut watcher = RecommendedWatcher::new(tx, Config::default())?;

    let filter = WatchFilter::with_filter(
        Arc::new(|path: &Path| !path.to_string_lossy().contains("/.git/objects")),
        Arc::new(|path: &Path| !path.to_string_lossy().contains("/.git/objects")),
    );
    watcher.watch_filtered(dir.path(), RecursiveMode::Recursive, filter)?;

    // Give the watcher a moment to settle before generating events.
    std::thread::sleep(Duration::from_millis(100));

    let head = dir.path().join(".git").join("HEAD");
    fs::write(&head, b"ref: refs/heads/feature\n")?;

    let blob = dir
        .path()
        .join(".git")
        .join("objects")
        .join("ab")
        .join("blob");
    fs::write(&blob, b"updated\n")?;

    let regular = dir.path().join("file.txt");
    fs::write(&regular, b"world\n")?;

    let events = collect_events(&rx, Duration::from_secs(2));
    let observed = paths_under(&events, dir.path());

    assert!(
        observed.iter().any(|p| p == &head),
        "expected event for `.git/HEAD`, got: {observed:#?}"
    );
    assert!(
        observed.iter().any(|p| p == &regular),
        "expected event for `file.txt`, got: {observed:#?}"
    );
    assert!(
        !observed.iter().any(|p| p == &blob),
        "should not have received an event for `.git/objects/<blob>`, got: {observed:#?}"
    );

    Ok(())
}

/// With the old single-predicate behaviour, a filter that excluded `.git/`
/// the directory but allowed `.git/HEAD` the file would prune the entire
/// `.git/` subtree at registration time and miss every event under it.
/// The directory-watch/event-emit split lets us express the case correctly:
/// watch through `.git/` (so we reach `HEAD`), but only emit events for
/// `HEAD` itself.
#[test]
fn should_emit_event_allows_subset_of_watched_paths(
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    build_fake_repo(dir.path())?;

    let (tx, rx) = std::sync::mpsc::channel();
    let mut watcher = RecommendedWatcher::new(tx, Config::default())?;

    let filter = WatchFilter::with_filter(
        Arc::new(|_: &Path| true),
        Arc::new(|path: &Path| {
            path.file_name().map(|n| n == "HEAD").unwrap_or(false)
                && path.to_string_lossy().contains("/.git/")
        }),
    );
    watcher.watch_filtered(dir.path(), RecursiveMode::Recursive, filter)?;

    std::thread::sleep(Duration::from_millis(100));

    let head = dir.path().join(".git").join("HEAD");
    fs::write(&head, b"ref: refs/heads/feature\n")?;

    let regular = dir.path().join("file.txt");
    fs::write(&regular, b"world\n")?;

    let events = collect_events(&rx, Duration::from_secs(2));
    let observed = paths_under(&events, dir.path());

    assert!(
        observed.iter().any(|p| p == &head),
        "expected event for `.git/HEAD` (watched through `.git/`), got: {observed:#?}"
    );
    assert!(
        !observed.iter().any(|p| p == &regular),
        "did not expect an event for `file.txt` (should_emit_event rejects it), got: {observed:#?}"
    );

    Ok(())
}
