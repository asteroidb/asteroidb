#[test]
fn unsafe_hole_jump_opt_in_fails_before_opening_state() {
    let dir = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_asteroidb"))
        .env("ASTEROIDB_GC_HOLE_JUMP", "1")
        .env("ASTEROIDB_DATA_DIR", dir.path())
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("ASTEROIDB_GC_HOLE_JUMP is unavailable")
    );
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}
