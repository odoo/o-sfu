use super::*;

#[test]
fn projection_preserves_line_endings_and_reuses_bundled_targets() -> Result<()> {
    let target = SocketAddr::from((Ipv4Addr::LOCALHOST, 40_000));
    let relay = SocketAddr::from((Ipv4Addr::LOCALHOST, 50_000));
    for (ending, final_newline) in [("\r\n", true), ("\n", true), ("\n", false)] {
        let mut input = [
            "v=0",
            "a=group:BUNDLE audio video",
            "m=audio 9 UDP/TLS/RTP/SAVPF 96",
            "a=mid:audio",
            "a=candidate:one 1 UDP 100 127.0.0.1 40000 typ host generation 0",
            "m=video 9 UDP/TLS/RTP/SAVPF 97",
            "a=mid:video",
            "a=candidate:two 1 UDP 90 127.0.0.1 40000 typ host generation 0",
        ]
        .join(ending);
        if final_newline {
            input.push_str(ending);
        }
        let projection = Projection::parse(&input)?;
        assert_eq!(projection.targets().collect::<Vec<_>>(), [target]);
        let output = projection.render(|address| (address == target).then_some(relay))?;
        assert_eq!(
            output,
            input.replace("127.0.0.1 40000", "127.0.0.1 50000"),
            "ending {ending:?}, final newline {final_newline}"
        );
    }
    Ok(())
}
