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
    def _is_segment_semicolon(segment: BaseSegment) -> bool:
        """Check if a segment is a semicolon statement terminator."""
        return segment.is_type("statement_terminator") and segment.raw == ";"

    @staticmethod
    def _get_last_statement(file_segment: BaseSegment) -> Optional[BaseSegment]:
        """Get the last statement from a file segment."""
        for seg in reversed(file_segment.segments):
            if seg.is_type("statement"):
                return seg
            if seg.is_type("batch"):
                for subseg in reversed(seg.segments):
                    if subseg.is_type("statement"):
                        return subseg
        # If no direct statement found, look recursively (e.g., T-SQL batch structure)
        statements = list(file_segment.recursive_crawl("statement"))
        return statements[-1] if statements else None

    def _has_final_non_semicolon_terminator(self, file_segment: BaseSegment) -> bool:
        """Check if a statement has a non-semicolon terminator at the end."""
        last_statement = self._get_last_statement(file_segment)

        if last_statement is not None:
            statement_terminators = list(
                last_statement.recursive_crawl("statement_terminator")
            )
            if statement_terminators:
                last_terminator = statement_terminators[-1]
                return not self._is_segment_semicolon(last_terminator)
        return False

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
        # Only handle actual semicolons. Other dialects define non-semicolon
        # statement terminators at the file level (e.g. MySQL's ~ and Databricks'
        # command-cell marker) which should not be repositioned by this rule.
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

    def _handle_repeated_semicolons(
        self, target_segment: RawSegment, parent_segment: BaseSegment
    ) -> Optional[LintResult]:
        """Handle consecutive semicolons: delete extras and whitespace between them."""
        segments = parent_segment.segments
        try:
            start_idx = segments.index(target_segment)
        except ValueError:
            return None

        # Collect the run of extra ; and spaces (not newlines) after target_segment.
        run = []
        for seg in segments[start_idx + 1:]:
            if (seg.is_type("statement_terminator") and seg.raw == ";") or seg.is_type("whitespace"):
                run.append(seg)
            else:
                break

        if not any(s.is_type("statement_terminator") for s in run):
            return None

        return LintResult(
            anchor=target_segment,
            fixes=[LintFix.delete(s) for s in run],
        )

    def _handle_semicolon_same_line(
        self,
        target_segment: RawSegment,
        parent_segment: BaseSegment,
        info: SegmentMoveContext,
    ) -> Optional[LintResult]:
        if not info.before_segment:
            return None

        fixes = self._create_semicolon_and_delete_whitespace(
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
            fixes=fixes,
        )

    def _handle_semicolon_newline(
        self,
        target_segment: RawSegment,
        parent_segment: BaseSegment,
        info: SegmentMoveContext,
    ) -> Optional[LintResult]:
        # Adjust before_segment and anchor_segment for preceding inline
        # comments. Inline comments can contain noqa logic so we need to add the
        # newline after the inline comment.
        before_segment, anchor_segment = self._handle_preceding_inline_comments(
            info.before_segment, info.anchor_segment
        )

        if (len(before_segment) == 1) and all(
            s.is_type("newline") for s in before_segment
        ):
            return None

        # This handles an edge case in which an inline comment comes after
        # the semi-colon.
        anchor_segment = self._handle_trailing_inline_comments(
            parent_segment, anchor_segment
        )
        fixes = []
        if anchor_segment is target_segment:
            fixes.append(
                LintFix.replace(
                    anchor_segment,
                    [
                        NewlineSegment(),
                        SymbolSegment(raw=";", type="statement_terminator"),
                    ],
                )
            )
        else:
            fixes.extend(
                self._create_semicolon_and_delete_whitespace(
                    target_segment,
                    parent_segment,
                    anchor_segment,
                    info.whitespace_deletions,
                    [
                        NewlineSegment(),
                        SymbolSegment(raw=";", type="statement_terminator"),
                    ],
                )
            )
        return LintResult(
            anchor=anchor_segment,
            fixes=fixes,
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
        last_code = next((s for s in reversed(parent_segment.segments) if s.is_code), None)
        if last_code is None:
            return None

        semi_colon_exist_flag = any(
            s.is_type("statement_terminator") for s in parent_segment.segments
        )
        is_one_line = self._is_one_line_statement(parent_segment, last_code)

        before_segment = []
        anchor_segment = trigger_segment = parent_segment.segments[-1]
        for segment in reversed(parent_segment.segments):
            anchor_segment = segment
            if segment.is_code:
                break
            elif not segment.is_meta:
                before_segment.append(segment)
            trigger_segment = segment

        self.logger.debug("Trigger on: %s", trigger_segment)
        self.logger.debug("Anchoring on: %s", anchor_segment)

        semicolon_newline = self.multiline_newline if not is_one_line else False

        if not semi_colon_exist_flag:
            if semicolon_newline:
                before_segment, anchor_segment = self._handle_preceding_inline_comments(
                    before_segment, anchor_segment
                )
                self.logger.debug("Revised anchor on: %s", anchor_segment)
            create_segments: list[BaseSegment] = []
            if semicolon_newline:
                create_segments.append(NewlineSegment())
            create_segments.append(SymbolSegment(raw=";", type="statement_terminator"))
            fixes = [
                LintFix.create_after(
                    self._choose_anchor_segment(
                        parent_segment, "create_after", anchor_segment, filter_meta=True
                    ),
                    create_segments,
                )
            ]
            return LintResult(anchor=trigger_segment, fixes=fixes)
        return None

    def _handle_missing_semicolon(
        self, statement_segment: BaseSegment, parent_segment: BaseSegment
    ) -> Optional[LintResult]:
        """Handle a statement that is completely missing a semicolon."""
        is_one_line = self._is_one_line_statement(parent_segment, statement_segment)
        semicolon_newline = self.multiline_newline and not is_one_line

        # Collect non-code, non-meta segments after this statement (before the next
        # code segment) to detect same-line inline comments that must stay before \n;
        anchor_segment: BaseSegment = statement_segment
        before_segment: list[BaseSegment] = []
        found = False
        for segment in parent_segment.segments:
            if segment is statement_segment:
                found = True
                continue
            if found:
                if segment.is_code:
                    break
                elif not segment.is_meta:
                    before_segment.append(segment)

        if semicolon_newline:
            _, anchor_segment = self._handle_preceding_inline_comments(
                before_segment, anchor_segment
            )

        create_segments: list[BaseSegment] = []
        if semicolon_newline:
            create_segments.append(NewlineSegment())
        create_segments.append(SymbolSegment(raw=";", type="statement_terminator"))

        # Use the anchor itself as the root to prevent hoisting up to the
        # batch/file level.  Hoisting would place the semicolon *outside* the
        # batch, causing the rule to re-fire on subsequent fix iterations.
        final_anchor = self._choose_anchor_segment(
            anchor_segment, "create_after", anchor_segment, filter_meta=True
        )
        return LintResult(
            anchor=statement_segment,
            fixes=[LintFix.create_after(final_anchor, create_segments)],
        )

    def _eval(self, context: RuleContext) -> list[LintResult]:
        """Statements must end with a semi-colon."""
        # Config type hints
        self.multiline_newline: bool
        self.require_final_semicolon: bool

        # We should only be dealing with a root segment
        assert context.segment.is_type("file")
        results = []

        # Process statements at the file level AND inside any top-level batch
        # segments (T-SQL, Oracle, and similar dialects wrap statements in batches).
        containers: list[BaseSegment] = [context.segment]
        for seg in context.segment.segments:
            if seg.is_type("batch"):
                containers.append(seg)

        for container in containers:
            statements_needing_semicolons: list[BaseSegment] = []

            for seg in container.segments:
                res = None

                if seg.is_type("statement"):
                    # Skip statements whose last raw segment is already a ';'
                    # statement_terminator (e.g. PL/SQL blocks ending with END;).
                    # Those statements carry their own terminator internally.
                    raw_segs = seg.raw_segments
                    if not (
                        raw_segs
                        and self._is_segment_semicolon(raw_segs[-1])
                    ):
                        statements_needing_semicolons.append(seg)

                # First we can simply handle the case of existing semi-colon alignment.
                if seg.is_type("statement_terminator"):
                    # If it's a terminator then we know it's a raw.
                    seg = cast(RawSegment, seg)
                    self.logger.debug("Handling semi-colon: %s", seg)
                    res = self._handle_semicolon(seg, container)

                    if statements_needing_semicolons:
                        statements_needing_semicolons.pop()

                if res:
                    results.append(res)

            if self.require_final_semicolon:
                for statement in statements_needing_semicolons:
                    res = self._handle_missing_semicolon(statement, container)
                    if res:
                        results.append(res)

        return results
