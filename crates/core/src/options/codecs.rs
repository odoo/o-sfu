//! codec policy shared by server config and RTC transport construction
//!
//! server configuration parses operator input into these values then
//! `MediaTransport::build` compiles them into one private RTP profile

use bitflags::bitflags;
use o_sfu_rfc::rtp::codec_name;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct MediaCodecSet: u16 {
        const OPUS = 1 << 0;
        const PCMU = 1 << 1;
        const PCMA = 1 << 2;
        const VP8 = 1 << 3;
        const H264 = 1 << 4;
        const H265 = 1 << 5;
        const VP9 = 1 << 6;
        const AV1 = 1 << 7;
    }
}

/// Codec enablement for RTP profile compilation.
///
/// [`Default`] enables Opus and VP8. [`CodecPreferences`] determines their
/// compilation order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaCodecFlags {
    enabled: MediaCodecSet,
}

macro_rules! media_codec_accessors {
    ($($enabled:ident => $with:ident => $flag:ident),+ $(,)?) => {
        $(
            #[doc = concat!("returns whether `", stringify!($flag), "` may enter the compiled RTP profile")]
            #[must_use]
            pub fn $enabled(self) -> bool {
                self.enabled.contains(MediaCodecSet::$flag)
            }

            #[doc = concat!("returns a copy with `", stringify!($flag), "` enabled or disabled for profile compilation")]
            #[must_use]
            pub fn $with(self, enabled: bool) -> Self {
                self.with_flag(MediaCodecSet::$flag, enabled)
            }
        )+
    };
}

impl MediaCodecFlags {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            enabled: MediaCodecSet::empty(),
        }
    }

    #[must_use]
    fn with_flag(mut self, flag: MediaCodecSet, enabled: bool) -> Self {
        if enabled {
            self.enabled.insert(flag);
        } else {
            self.enabled.remove(flag);
        }
        self
    }

    media_codec_accessors!(
        opus_enabled => with_opus => OPUS,
        pcmu_enabled => with_pcmu => PCMU,
        pcma_enabled => with_pcma => PCMA,
        vp8_enabled => with_vp8 => VP8,
        h264_enabled => with_h264 => H264,
        h265_enabled => with_h265 => H265,
        vp9_enabled => with_vp9 => VP9,
        av1_enabled => with_av1 => AV1,
    );
}

impl Default for MediaCodecFlags {
    fn default() -> Self {
        Self {
            enabled: MediaCodecSet::OPUS | MediaCodecSet::VP8,
        }
    }
}

/// audio codec entry used to rank the negotiated audio capability surface
///
/// the private RTP profile compiler filters this order through
/// [`MediaCodecFlags`]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioCodecPreference {
    /// default audio codec for browser RTC sessions
    Opus,
    /// g.711 mu-law compatibility codec
    Pcmu,
    /// g.711 a-law compatibility codec
    Pcma,
}

impl AudioCodecPreference {
    /// canonical operator-configuration token for this codec
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Opus => codec_name::OPUS,
            Self::Pcmu => codec_name::PCMU,
            Self::Pcma => codec_name::PCMA,
        }
    }

    /// returns whether this preference enters the compiled RTP profile
    #[must_use]
    pub fn enabled_by(self, flags: MediaCodecFlags) -> bool {
        match self {
            Self::Opus => flags.opus_enabled(),
            Self::Pcmu => flags.pcmu_enabled(),
            Self::Pcma => flags.pcma_enabled(),
        }
    }
}

/// video codec entry used to rank the negotiated video capability surface
///
/// the private RTP profile compiler filters this order through
/// [`MediaCodecFlags`] before installing concrete payload configurations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoCodecPreference {
    /// default video codec for browser RTC sessions
    Vp8,
    /// browser-compatible h264 path with the shared payload contract
    H264,
    /// optional h265 capability controlled by runtime flags
    H265,
    /// optional vp9 capability controlled by runtime flags
    Vp9,
    /// optional av1 capability controlled by runtime flags
    Av1,
}

impl VideoCodecPreference {
    /// canonical operator-configuration token for this codec
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Vp8 => codec_name::VP8,
            Self::H264 => codec_name::H264,
            Self::H265 => codec_name::H265,
            Self::Vp9 => codec_name::VP9,
            Self::Av1 => codec_name::AV1,
        }
    }

    /// returns whether this preference enters the compiled RTP profile
    #[must_use]
    pub fn enabled_by(self, flags: MediaCodecFlags) -> bool {
        match self {
            Self::Vp8 => flags.vp8_enabled(),
            Self::H264 => flags.h264_enabled(),
            Self::H265 => flags.h265_enabled(),
            Self::Vp9 => flags.vp9_enabled(),
            Self::Av1 => flags.av1_enabled(),
        }
    }
}

/// Audio and video codec ordering for RTP profile compilation.
///
/// Partial orders should use [`Self::with_audio_order`] and
/// [`Self::with_video_order`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecPreferences {
    audio: [AudioCodecPreference; 3],
    video: [VideoCodecPreference; 5],
}

impl CodecPreferences {
    /// canonical audio order used when operators do not override preferences
    pub const DEFAULT_AUDIO: [AudioCodecPreference; 3] = [
        AudioCodecPreference::Opus,
        AudioCodecPreference::Pcmu,
        AudioCodecPreference::Pcma,
    ];
    /// canonical video order used when operators do not override preferences
    pub const DEFAULT_VIDEO: [VideoCodecPreference; 5] = [
        VideoCodecPreference::Vp8,
        VideoCodecPreference::H264,
        VideoCodecPreference::H265,
        VideoCodecPreference::Vp9,
        VideoCodecPreference::Av1,
    ];

    /// Stores caller-supplied complete audio and video orders.
    ///
    /// Each array must contain every corresponding codec exactly once. This
    /// constructor does not validate the permutation.
    #[must_use]
    pub const fn new(audio: [AudioCodecPreference; 3], video: [VideoCodecPreference; 5]) -> Self {
        Self { audio, video }
    }

    /// Places each distinct `preferred` codec first in encounter order.
    ///
    /// Every omitted codec follows in canonical default order. The previous
    /// audio order is not used.
    #[must_use]
    pub fn with_audio_order(self, preferred: &[AudioCodecPreference]) -> Self {
        Self {
            audio: complete_codec_order(preferred, Self::DEFAULT_AUDIO),
            ..self
        }
    }

    /// Places each distinct `preferred` codec first in encounter order.
    ///
    /// Every omitted codec follows in canonical default order. The previous
    /// video order is not used.
    #[must_use]
    pub fn with_video_order(self, preferred: &[VideoCodecPreference]) -> Self {
        Self {
            video: complete_codec_order(preferred, Self::DEFAULT_VIDEO),
            ..self
        }
    }

    /// complete audio order after defaults filled any omitted codecs
    #[must_use]
    pub const fn audio_order(self) -> [AudioCodecPreference; 3] {
        self.audio
    }

    /// complete video order after defaults filled any omitted codecs
    #[must_use]
    pub const fn video_order(self) -> [VideoCodecPreference; 5] {
        self.video
    }
}

impl Default for CodecPreferences {
    fn default() -> Self {
        Self::new(Self::DEFAULT_AUDIO, Self::DEFAULT_VIDEO)
    }
}

fn complete_codec_order<T, const N: usize>(preferred: &[T], default: [T; N]) -> [T; N]
where
    T: Copy + Eq,
{
    let mut output = default;
    let mut len = 0;
    // `default` must contain every codec exactly once. Once `len == N`, every
    // remaining item is therefore a duplicate.
    for codec in preferred.iter().copied().chain(default) {
        if contains_codec(&output, len, codec) {
            continue;
        }
        if let Some(slot) = output.get_mut(len) {
            *slot = codec;
            len += 1;
        }
    }
    output
}

fn contains_codec<T, const N: usize>(codecs: &[T; N], len: usize, needle: T) -> bool
where
    T: Copy + Eq,
{
    codecs.iter().take(len).any(|codec| *codec == needle)
}
