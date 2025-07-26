"""Implementation of Rule CV06."""

from collections.abc import Sequence
from typing import NamedTuple, Optional, cast

from sqlfluff.core.parser import BaseSegment, NewlineSegment, RawSegment, SymbolSegment
from sqlfluff.core.rules import BaseRule, LintFix, LintResult, RuleContext
from sqlfluff.core.rules.crawlers import RootOnlyCrawler
from sqlfluff.utils.functional import Segments, sp


class SegmentMoveContext(NamedTuple):
    """Context information for moving a segment."""

    anchor_segment: RawSegment
    is_one_line: bool
    before_segment: Segments
    whitespace_deletions: Segments


class Rule_CV06(BaseRule):
    """Statements must end with a semi-colon.

    **Anti-pattern**

    A statement is not immediately terminated with a semi-colon. The ``•`` represents
    space.

    .. code-block:: sql
       :force:

        SELECT
            a
        FROM foo

        ;

        SELECT
            b
        FROM bar••;

    **Best practice**

    Immediately terminate the statement with a semi-colon.

    .. code-block:: sql
       :force:

        SELECT
            a
        FROM foo;
    """

    name = "convention.terminator"
    aliases = ("L052",)
    groups = ("all", "convention")
    config_keywords = ["multiline_newline", "require_final_semicolon"]
    crawl_behaviour = RootOnlyCrawler()
    is_fix_compatible = True

    @staticmethod
    def _handle_preceding_inline_comments(
        before_segment: Sequence[BaseSegment], anchor_segment: BaseSegment
    ):
        """Adjust segments to not move preceding inline comments.

        We don't want to move inline comments that are on the same line
        as the preceding code segment as they could contain noqa instructions.
        """
        # See if we have a preceding inline comment on the same line as the preceding
        # segment.
        same_line_comment = next(
            (
                s
                for s in before_segment
                if s.is_comment
                and not s.is_type("block_comment")
                and s.pos_marker
                and s.pos_marker.working_line_no
                # We don't need to handle the case where raw_segments is empty
                # because it never is. It's either a segment with raw children
                # or a raw segment which returns [self] as raw_segments.
                == anchor_segment.raw_segments[-1].pos_marker.working_line_no
            ),
            None,
        )
        # If so then make that our new anchor segment and adjust
        # before_segment accordingly.
        if same_line_comment:
            anchor_segment = same_line_comment
            before_segment = before_segment[: before_segment.index(same_line_comment)]

        return before_segment, anchor_segment

    @staticmethod
    def _handle_trailing_inline_comments(
        parent_segment: BaseSegment, anchor_segment: BaseSegment
    ) -> BaseSegment:
        """Adjust anchor_segment to not move trailing inline comment.

        We don't want to move inline comments that are on the same line
        as the preceding code segment as they could contain noqa instructions.
        """
        # See if we have a trailing inline comment on the same line as the preceding
        # segment.
        for comment_segment in parent_segment.recursive_crawl("comment"):
            assert comment_segment.pos_marker
            assert anchor_segment.pos_marker
            if (
                comment_segment.pos_marker.working_line_no
                == anchor_segment.pos_marker.working_line_no
            ) and (not comment_segment.is_type("block_comment")):
                anchor_segment = comment_segment

        return anchor_segment

    @staticmethod
    def _is_one_line_statement(
        parent_segment: BaseSegment, segment: BaseSegment
    ) -> bool:
        """Check if the statement containing the provided segment is one line."""
        # Find statement segment containing the current segment.
        statement_segment = next(
            (
                ps.segment
                for ps in (parent_segment.path_to(segment) or [])
                if ps.segment.is_type("statement")
            ),
            None,
        )
        if statement_segment is None:  # pragma: no cover
            # If we can't find a parent statement segment then don't try anything
            # special.
            return False

        if not any(statement_segment.recursive_crawl("newline")):
            # Statement segment has no newlines therefore starts and ends on the same
            # line.
            return True

        return False

    def _get_segment_move_context(
        self, target_segment: RawSegment, parent_segment: BaseSegment
    ) -> SegmentMoveContext:
        # Locate the segment to be moved (i.e. context.segment) and search back
        # over the raw stack to find the end of the preceding statement.
        reversed_raw_stack = Segments(*parent_segment.raw_segments).reversed()
        before_code = reversed_raw_stack.select(
            loop_while=sp.not_(sp.is_code()), start_seg=target_segment
        )
        before_segment = before_code.select(sp.not_(sp.is_meta()))
        # We're selecting from the raw stack, so we know that before_code is
        # made of RawSegment elements.
        anchor_segment = (
            cast(RawSegment, before_code[-1]) if before_code else target_segment
        )
        first_code = reversed_raw_stack.select(
            sp.is_code(), start_seg=target_segment
        ).first()
        self.logger.debug("Semicolon: first_code: %s", first_code)
        is_one_line = (
            self._is_one_line_statement(parent_segment, first_code[0])
            if first_code
            else False
        )

        # We can tidy up any whitespace between the segment
        # and the preceding code/comment segment.
        # Don't mess with comment spacing/placement.
        whitespace_deletions = before_segment.select(loop_while=sp.is_whitespace())
        return SegmentMoveContext(
            anchor_segment, is_one_line, before_segment, whitespace_deletions
        )

    def _handle_semicolon(
        self, target_segment: RawSegment, parent_segment: BaseSegment
    ) -> Optional[LintResult]:
        # Only handle actual semicolons, ignore other terminators like Oracle's /
        if target_segment.raw != ";":
            return None

        repeated_semicolon_result = self._handle_repeated_semicolons(target_segment, parent_segment)
        if repeated_semicolon_result:
            return repeated_semicolon_result

        info = self._get_segment_move_context(target_segment, parent_segment)
        semicolon_newline = self.multiline_newline if not info.is_one_line else False
        self.logger.debug("Semicolon Newline: %s", semicolon_newline)

        # Semi-colon on same line.
        if not semicolon_newline:
            return self._handle_semicolon_same_line(
                target_segment, parent_segment, info
            )
        # Semi-colon on new line.
        else:
            return self._handle_semicolon_newline(target_segment, parent_segment, info)

    def _is_semicolon_terminator(self, segment: BaseSegment) -> bool:
        """Check if segment is a semicolon terminator."""
        return segment.is_type("statement_terminator") and segment.raw == ";"

    def _is_safe_to_remove_between_semicolons(self, segment: BaseSegment) -> bool:
        """Check if segment is safe to remove when cleaning up between consecutive semicolons."""
        return segment.is_type("whitespace") or segment.is_type("newline")

    def _handle_repeated_semicolons(
        self, target_segment: RawSegment, parent_segment: BaseSegment
    ) -> Optional[LintResult]:
        """Handle multiple consecutive semicolons by replacing with single semicolon."""
        # Find the position of the current semicolon in the parent's segments
        segments = parent_segment.segments
        try:
            current_idx = segments.index(target_segment)
        except ValueError:
            return None

        consecutive_semicolons = [target_segment]
        current_search_idx = current_idx + 1

        while current_search_idx < len(segments):
            current_segment = segments[current_search_idx]
            if self._is_semicolon_terminator(current_segment):
                consecutive_semicolons.append(current_segment)
                current_search_idx += 1
            elif current_segment.is_type("whitespace"):
                next_non_whitespace_idx = current_search_idx + 1
                while (next_non_whitespace_idx < len(segments) and
                       segments[next_non_whitespace_idx].is_type("whitespace")):
                    next_non_whitespace_idx += 1

                has_semicolon_after_whitespace = (next_non_whitespace_idx < len(segments) and
                    self._is_semicolon_terminator(segments[next_non_whitespace_idx]))
                
                if has_semicolon_after_whitespace:
                    current_search_idx += 1
                else:
                    current_search_idx = len(segments)
            else:
                current_search_idx = len(segments)

        if len(consecutive_semicolons) > 1:
            extra_semicolon_removal_fixes = []
            for extra_semicolon in consecutive_semicolons[1:]:
                extra_semicolon_removal_fixes.append(LintFix.delete(extra_semicolon))

            for segment_idx in range(current_idx + 1, current_search_idx):
                if (segment_idx < len(segments) and
                    segments[segment_idx].is_type("whitespace") and
                    segments[segment_idx] not in consecutive_semicolons):
                    extra_semicolon_removal_fixes.append(LintFix.delete(segments[segment_idx]))

            return LintResult(
                anchor=target_segment,
                fixes=extra_semicolon_removal_fixes,
            )

        return None

    def _handle_semicolon_same_line(
        self,
        target_segment: RawSegment,
        parent_segment: BaseSegment,
        info: SegmentMoveContext,
    ) -> Optional[LintResult]:
        if not info.before_segment:
            return None

        semicolon_placement_fixes = self._create_semicolon_and_delete_whitespace(
            target_segment,
            parent_segment,
            info.anchor_segment,
            info.whitespace_deletions,
            [
                SymbolSegment(raw=";", type="statement_terminator"),
            ],
        )
        return LintResult(
            anchor=info.anchor_segment,
            fixes=semicolon_placement_fixes,
        )

    def _handle_semicolon_newline(
        self,
        target_segment: RawSegment,
        parent_segment: BaseSegment,
        info: SegmentMoveContext,
    ) -> Optional[LintResult]:
        adjusted_before_segments, adjusted_anchor = self._handle_preceding_inline_comments(
            info.before_segment, info.anchor_segment
        )

        if (len(adjusted_before_segments) == 1) and all(
            s.is_type("newline") for s in adjusted_before_segments
        ):
            return None

        anchor_after_trailing_comments = self._handle_trailing_inline_comments(
            parent_segment, adjusted_anchor
        )
        semicolon_placement_fixes = []
        if anchor_after_trailing_comments is target_segment:
            semicolon_placement_fixes.append(
                LintFix.replace(
                    anchor_after_trailing_comments,
                    [
                        NewlineSegment(),
                        SymbolSegment(raw=";", type="statement_terminator"),
                    ],
                )
            )
        else:
            semicolon_placement_fixes.extend(
                self._create_semicolon_and_delete_whitespace(
                    target_segment,
                    parent_segment,
                    anchor_after_trailing_comments,
                    info.whitespace_deletions,
                    [
                        NewlineSegment(),
                        SymbolSegment(raw=";", type="statement_terminator"),
                    ],
                )
            )
        return LintResult(
            anchor=anchor_after_trailing_comments,
            fixes=semicolon_placement_fixes,
        )

    def _create_semicolon_and_delete_whitespace(
        self,
        target_segment: BaseSegment,
        parent_segment: BaseSegment,
        anchor_segment: BaseSegment,
        whitespace_deletions: Segments,
        create_segments: list[BaseSegment],
    ) -> list[LintFix]:
        anchor_segment = self._choose_anchor_segment(
            parent_segment, "create_after", anchor_segment, filter_meta=True
        )
        lintfix_fn = LintFix.create_after
        whitespace_deletion_set = set(whitespace_deletions)
        if anchor_segment in whitespace_deletion_set:
            # Can't delete() and create_after() the same segment. Use replace()
            # instead.
            lintfix_fn = LintFix.replace
            whitespace_deletions = whitespace_deletions.select(
                lambda seg: seg is not anchor_segment
            )
        fixes = [
            lintfix_fn(
                anchor_segment,
                create_segments,
            ),
            LintFix.delete(
                target_segment,
            ),
        ]
        fixes.extend(LintFix.delete(d) for d in whitespace_deletions)
        return fixes

    def _ensure_final_semicolon(
        self, parent_segment: BaseSegment
    ) -> Optional[LintResult]:
        last_code_segment = next((seg for seg in reversed(parent_segment.segments) if seg.is_code), None)
        if last_code_segment is None:
            return None

        final_semicolon_exists = any(seg.is_type("statement_terminator") for seg in reversed(parent_segment.segments))
        statement_is_single_line = self._is_one_line_statement(parent_segment, last_code_segment)
        
        non_meta_segments_before_code = []
        anchor_segment = parent_segment.segments[-1]
        trigger_segment = parent_segment.segments[-1]
        
        for segment in reversed(parent_segment.segments):
            anchor_segment = segment
            if segment.is_code:
                break
            elif not segment.is_meta:
                non_meta_segments_before_code.append(segment)
            trigger_segment = segment

        self.logger.debug("Trigger on: %s", trigger_segment)
        self.logger.debug("Anchoring on: %s", anchor_segment)

        should_use_newline_before_semicolon = self.multiline_newline if not statement_is_single_line else False

        if not final_semicolon_exists:
            if not should_use_newline_before_semicolon:
                fixes = [
                    LintFix.create_after(
                        self._choose_anchor_segment(
                            parent_segment,
                            "create_after",
                            anchor_segment,
                            filter_meta=True,
                        ),
                        [
                            SymbolSegment(raw=";", type="statement_terminator"),
                        ],
                    )
                ]
            else:
                adjusted_segments, adjusted_anchor = self._handle_preceding_inline_comments(
                    non_meta_segments_before_code, anchor_segment
                )
                self.logger.debug("Revised anchor on: %s", adjusted_anchor)
                fixes = [
                    LintFix.create_after(
                        self._choose_anchor_segment(
                            parent_segment,
                            "create_after",
                            adjusted_anchor,
                            filter_meta=True,
                        ),
                        [
                            NewlineSegment(),
                            SymbolSegment(raw=";", type="statement_terminator"),
                        ],
                    )
                ]
            return LintResult(
                anchor=trigger_segment,
                fixes=fixes,
            )
        return None

    def _handle_missing_semicolon(
        self, statement_segment: BaseSegment, parent_segment: BaseSegment
    ) -> Optional[LintResult]:
        """Handle a statement that is completely missing a semicolon."""

        last_non_meta_segment = next((seg for seg in reversed(statement_segment.segments) if not seg.is_meta), None)

        if not last_non_meta_segment:
            return None

        statement_is_single_line = self._is_one_line_statement(parent_segment, statement_segment)
        should_use_newline_before_semicolon = self.multiline_newline if not statement_is_single_line else False

        anchor_after_inline_comments = self._handle_trailing_inline_comments(parent_segment, last_non_meta_segment)

        if should_use_newline_before_semicolon:
            segments_to_add = [
                NewlineSegment(),
                SymbolSegment(raw=";", type="statement_terminator"),
            ]
        else:
            segments_to_add = [
                SymbolSegment(raw=";", type="statement_terminator"),
            ]

        final_anchor_segment = self._choose_anchor_segment(
            parent_segment, "create_after", anchor_after_inline_comments, filter_meta=True
        )

        missing_semicolon_fixes = [
            LintFix.create_after(final_anchor_segment, segments_to_add)
        ]

        return LintResult(
            anchor=last_non_meta_segment,
            fixes=missing_semicolon_fixes,
        )

    def _eval(self, context: RuleContext) -> list[LintResult]:
        """Statements must end with a semi-colon."""
        # Config type hints
        self.multiline_newline: bool
        self.require_final_semicolon: bool

        # We should only be dealing with a root segment
        assert context.segment.is_type("file")
        results = []

        statements_needing_semicolons = []

        for idx, seg in enumerate(context.segment.segments):
            res = None

            if seg.is_type("statement"):
                statements_needing_semicolons.append(seg)

            # First we can simply handle the case of existing semi-colon alignment.
            if seg.is_type("statement_terminator"):
                # If it's a terminator then we know it's a raw.
                seg = cast(RawSegment, seg)
                self.logger.debug("Handling semi-colon: %s", seg)
                res = self._handle_semicolon(seg, context.segment)

                if statements_needing_semicolons:
                    statements_needing_semicolons.pop()
            # Otherwise handle the end of the file separately.
            elif (
                self.require_final_semicolon
                and idx == len(context.segment.segments) - 1
            ):
                self.logger.debug("Handling final segment: %s", seg)
                res = self._ensure_final_semicolon(context.segment)

            if res:
                results.append(res)

        if self.require_final_semicolon:
            last_statement = None
            reversed_segments = reversed(context.segment.segments)
            last_statement = next((seg for seg in reversed_segments if seg.is_type("statement")), None)

            for statement in statements_needing_semicolons:
                if statement != last_statement:
                    res = self._handle_missing_semicolon(statement, context.segment)
                    if res:
                        results.append(res)

        return results
