use crate::{Bitrate, engine::VideoLayoutIntent};

/// room policy applied to one published source
///
/// [`SourcePolicy`] is the source contract between application publish intent
/// and core room policy
/// it tells core what it may do with a source after
/// publish, but it does not name the product feature that created the stream
/// and it does not limit how many streams a user may publish
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourcePolicy {
    layout: Option<SourceLayoutPolicy>,
    adaptation: SourceAdaptationPolicy,
    active_speaker: Option<ActiveSpeakerPolicy>,
    video_bitrate_cap: Option<Bitrate>,
}

impl SourcePolicy {
    #[must_use]
    pub const fn new(
        layout: Option<SourceLayoutPolicy>,
        adaptation: SourceAdaptationPolicy,
        active_speaker: Option<ActiveSpeakerPolicy>,
    ) -> Self {
        Self {
            layout,
            adaptation,
            active_speaker,
            video_bitrate_cap: None,
        }
    }

    #[must_use]
    pub const fn with_video_bitrate_cap(self, max_bitrate: Bitrate) -> Self {
        Self {
            video_bitrate_cap: Some(max_bitrate),
            ..self
        }
    }

    #[must_use]
    pub const fn hidden() -> Self {
        Self::new(None, SourceAdaptationPolicy::None, None)
    }

    #[must_use]
    pub const fn layout(self) -> Option<SourceLayoutPolicy> {
        self.layout
    }

    #[must_use]
    pub const fn adaptation(self) -> SourceAdaptationPolicy {
        self.adaptation
    }

    #[must_use]
    pub const fn active_speaker(self) -> Option<ActiveSpeakerPolicy> {
        self.active_speaker
    }

    #[must_use]
    pub const fn video_bitrate_cap(self) -> Option<Bitrate> {
        self.video_bitrate_cap
    }
}

/// default receiver-layout role for one source
///
/// core combines publish intent, receiver layout preference and active-speaker
/// state to choose one [`SourceRoomPolicySelector`] per receiver/source route
/// sources without layout policy stay out of video budget planning
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceLayoutPolicy {
    visible_selector: SourceRoomPolicySelector,
    active_speaker_selector: Option<SourceRoomPolicySelector>,
}

impl SourceLayoutPolicy {
    #[must_use]
    pub const fn new(
        visible_selector: SourceRoomPolicySelector,
        active_speaker_selector: Option<SourceRoomPolicySelector>,
    ) -> Self {
        Self {
            visible_selector,
            active_speaker_selector,
        }
    }

    /// Resolves a receiver-specific layout role.
    ///
    /// Explicit [`VideoLayoutIntent`] wins so active-speaker observations cannot
    /// override that receiver's layout. Without explicit intent, an active speaker
    /// uses `active_speaker_selector` when configured. If neither explicit intent
    /// nor active-speaker selection applies, `visible_selector` is used.
    #[must_use]
    pub fn resolve(
        self,
        preference: Option<VideoLayoutIntent>,
        active_speaker: bool,
    ) -> SourceRoomPolicySelector {
        match preference {
            Some(VideoLayoutIntent::Pinned) => SourceRoomPolicySelector::Pinned,
            Some(VideoLayoutIntent::Featured) => SourceRoomPolicySelector::Featured,
            Some(VideoLayoutIntent::Hidden) => SourceRoomPolicySelector::Hidden,
            Some(VideoLayoutIntent::Overflow) => SourceRoomPolicySelector::Overflow,
            None if active_speaker => self
                .active_speaker_selector
                .unwrap_or(self.visible_selector),
            Some(VideoLayoutIntent::VisibleThumbnail) | None => self.visible_selector,
        }
    }
}

/// receiver bandwidth behavior for one published source
///
/// set by [`SourcePublishIntent`](crate::prelude::SourcePublishIntent) to decide
/// whether the source participates in receiver-video layer selection, route
/// pausing and over-budget diagnostics
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceAdaptationPolicy {
    /// keep this source out of receiver-video BWE control
    ///
    /// useful for audio or metadata-like sources that can route through normal
    /// subscriptions without spending visible-video budget
    None,
    /// let the receiver-video planner budget the source and choose an encoding
    ///
    /// useful for sources where thumbnail routes may downswitch or pause under
    /// receiver budget pressure
    ScalableVideo,
    /// keep readable detail ahead of normal thumbnail adaptation
    ///
    /// useful for text-heavy visual sources that stay on the highest advertised
    /// encoding until lower-priority routes are exhausted then pause if needed
    ReadableDetail,
}

/// active-speaker relationship declared for one source
///
/// publish intent decides which sources participate in transport-observed
/// speech relationships
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveSpeakerPolicy {
    group: ActiveSpeakerGroup,
    role: ActiveSpeakerSourceRole,
}

impl ActiveSpeakerPolicy {
    #[must_use]
    pub const fn new(group: ActiveSpeakerGroup, role: ActiveSpeakerSourceRole) -> Self {
        Self { group, role }
    }

    #[must_use]
    pub const fn group(self) -> ActiveSpeakerGroup {
        self.group
    }

    #[must_use]
    pub const fn role(self) -> ActiveSpeakerSourceRole {
        self.role
    }
}

/// active-speaker group id used to separate speech relationships
///
/// groups keep unrelated speech domains separate without teaching core about
/// application stream names
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveSpeakerGroup(u16);

impl ActiveSpeakerGroup {
    pub const MAIN: Self = Self(0);
}

/// role one source plays inside an active-speaker group
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveSpeakerSourceRole {
    /// transport observations from this source can mark its owner active
    ///
    /// detectors are usually audio-like and do not receive video layout
    /// treatment by themselves
    Detector,
    /// this source can receive active-speaker video treatment for its owner
    ///
    /// core promotes it only when a detector in the same group marks the same
    /// owner as active
    Promotable,
}

/// receiver-specific layout role before a concrete encoding is chosen
///
/// the budget planner reads this role to decide quality targets and pause order
/// transport code only sees the final packet gate after the role resolves
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceRoomPolicySelector {
    /// the receiver explicitly pinned this source
    ///
    /// pinned routes use featured quality and share the highest budget priority
    /// with featured routes
    Pinned,
    /// the receiver explicitly requested featured treatment for this source
    ///
    /// featured routes use featured quality and share the highest budget
    /// priority with pinned routes
    Featured,
    /// source policy says readable detail matters for this route
    ///
    /// explicit pinned or featured receiver intent outranks this role
    ReadableDetail,
    /// active-speaker policy promoted this source for the current receiver
    ///
    /// explicit receiver intent and readable detail outrank this role
    ActiveSpeaker,
    /// the source is visible as a secondary tile
    ///
    /// visible thumbnails downswitch and pause before higher-priority routes
    VisibleThumbnail,
    /// the receiver is subscribed but the source is not visible right now
    ///
    /// hidden routes skip visible-video budget and are first candidates for
    /// overload pause
    Hidden,
    /// the source is outside the receiver's visible tile set
    ///
    /// overflow routes behave like hidden routes but keep a distinct pause
    /// reason for diagnostics
    Overflow,
}

impl SourceRoomPolicySelector {
    #[must_use]
    pub const fn priority(self) -> SourceRoutePriority {
        match self {
            Self::Pinned | Self::Featured => SourceRoutePriority::PinnedOrFeatured,
            Self::ReadableDetail => SourceRoutePriority::ReadableDetail,
            Self::ActiveSpeaker => SourceRoutePriority::ActiveSpeaker,
            Self::VisibleThumbnail => SourceRoutePriority::VisibleThumbnail,
            Self::Hidden | Self::Overflow => SourceRoutePriority::HiddenOrOverflow,
        }
    }

    #[must_use]
    pub const fn uses_featured_quality(self) -> bool {
        matches!(
            self,
            Self::Pinned | Self::Featured | Self::ReadableDetail | Self::ActiveSpeaker
        )
    }

    #[must_use]
    pub const fn counts_toward_visible_budget(self) -> bool {
        !matches!(self, Self::Hidden | Self::Overflow)
    }
}

/// overload priority derived from a route's room-policy selector
///
/// Each downgrade or pause pass visits lower priorities first. All eligible
/// encoding downsteps precede whole-route pauses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SourceRoutePriority {
    /// explicit receiver intent
    ///
    /// pinned and featured routes outrank every other route and pause last
    PinnedOrFeatured,
    /// detail-preserving source policy
    ///
    /// readable-detail routes outrank active-speaker routes but stay below
    /// explicit pinned or featured receiver intent
    ReadableDetail,
    /// server-promoted active-speaker route
    ///
    /// explicit receiver intent and readable-detail source policy outrank this
    /// role
    ActiveSpeaker,
    /// visible secondary route
    ///
    /// visible thumbnails downswitch and then pause before higher-priority routes
    VisibleThumbnail,
    /// route that is not currently visible
    ///
    /// hidden and overflow routes are first to pause under receiver budget
    /// pressure
    HiddenOrOverflow,
}

/// reason why room policy withholds media for a subscribed route
///
/// subscription state can remain active while the packet gate is closed for
/// budget, layout or activation-cap reasons
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyPauseReason {
    /// the receiver budget cannot fit this route after cheaper layers were tried
    BudgetPressure,
    /// the receiver layout explicitly hides this source
    HiddenTile,
    /// the receiver layout puts this source outside the visible tile set
    OverflowTile,
    /// no negotiated encoding can be forwarded usefully
    MissingUsableLayer,
    /// the active-audio-speaker cap withheld this route
    AudioSpeakerLimit,
    /// the receiver deafened itself so no audio is delivered to it
    ReceiverDeafened,
    /// the per-receiver live-video cap withheld this route
    VideoDownloadLimit,
    /// the source bitrate cap withheld this route
    SourceBitrateLimit,
}

/// server-defined role for one published source encoding
///
/// the role lets the budget planner choose an encoding without reading
/// application stream names
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadLayerPolicyRole {
    /// highest useful quality for high-priority or detail-focused routes
    ///
    /// the planner avoids this as the first cheap fallback when a lower-cost
    /// encoding exists
    Featured,
    /// normal quality target for visible secondary video
    ///
    /// expected low-cost encoding before the planner considers pausing the route
    Thumbnail,
    /// lower-cost thumbnail rung below the normal thumbnail target
    ///
    /// reserved for upload ladders with more than two useful video encodings
    DegradedThumbnail,
}

impl UploadLayerPolicyRole {
    #[must_use]
    pub const fn as_wire_value(self) -> &'static str {
        match self {
            Self::Featured => "featured",
            Self::Thumbnail => "thumbnail",
            Self::DegradedThumbnail => "degradedThumbnail",
        }
    }
}
