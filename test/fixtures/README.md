# Test fixtures

## `eg-walker-conformance.json`

The first 250 of the 1,000 cases in `testdata/conformance.json` from
[josephg/eg-walker-reference](https://github.com/josephg/eg-walker-reference) at commit `7287f4b`. Each case is a set of
concurrent transactions and the text every replica must end up with. The reference implementation and Diamond Types
both produce these results, and FugueMax is believed to order text identically, so the tree strategy must reproduce
them exactly.

A transaction is `{ span, parents, agent, seqStart, ops }`:

- `span` is the range of local version numbers it covers.
- `parents` are the local version numbers of what it was made on top of.
- `ops` are `[position, removeCount, insertText]` edits, applied in order to the text at `parents`.

Used under the BSD 2-Clause license:

> Copyright (c) Joseph Gentle
>
> Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
> following conditions are met:
>
> 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
>    disclaimer.
> 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
>    following disclaimer in the documentation and/or other materials provided with the distribution.
>
> THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
> INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
> DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
> SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
> SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
> WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
> OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
