# EVENTS.md

One section per milestone. Fill each one in order as you go.

The prediction in milestone 2 should be written before you run the replay. That is the
part a TA will ask about.

Keep it short and specific. Point at operations, call sites, and what a caller
gets back.

---

## Milestone 1: what runs in the request path, and what runs after it

### The operations table

One row per operation. Read the code, not the file
names. "As wired today" is what the code does now. "Your call" is what you
would do if this were your service. List up to eight operations, and remove
the rows you don't use.

| # | Operation | As wired today | Your call | Why (in one line) |
|---|---|---|---|---|
| 1 | Validate the request and find a conflicting booking | Synchronous in `BookingService.book` | Synchronous | The caller must immediately learn whether the requested room and time are valid and available. |
| 2 | Calculate the booking price | Synchronous in `BookingService.book` | Synchronous | The confirmed booking returned to the caller must contain its authoritative price. |
| 3 | Index the booking for room search | Synchronous call from `BookingService.book` | Event-driven | Search is a derived view; an index outage should delay discoverability, not reject a valid reservation. |
| 4 | Save the confirmed booking | Synchronous in `BookingService.book` | Synchronous | The room must be durably held before the caller is told the booking succeeded. |
| 5 | Record the booking in the audit trail | Synchronous in `BookingService.book` | Synchronous | The acceptance record belongs to the operation's correctness boundary and should exist when success is returned. |
| 6 | Queue confirmation and cancellation email | Event-driven subscriber | Event-driven | Mail is a side effect that can be retried without blocking the booking decision. |
| 7 | Generate a receipt, schedule a reminder, and refresh dashboard stats | Event-driven subscribers | Event-driven | These are replaceable or recomputable projections that do not decide whether the room is held. |
| 8 | Record and pay an owed refund | Event-driven `RefundLedger` subscriber | Event-driven | Cancellation should release the room even if the payment provider is temporarily unavailable. |

### Two calls in detail

Pick two rows and give each a paragraph rather than a line. One of them has to
be the operation `TICKET.md` implicates.

**First operation and your call.** I would move search indexing (row 3) out of
the request path and subscribe it to the booking-confirmed event. The caller
needs a valid room to be atomically held and returned now, but does not need
the room-finder's derived search document before `book` returns. Today,
`SearchIndexClient.indexBooking` throws while the cluster is unavailable and
that exception escapes `BookingService.book`, so the page and kiosk see an
error and no booking is saved. Event-driven indexing would let booking succeed
and make search temporarily stale; a failed delivery could be retried from a
dead letter after the cluster recovers.

**Second operation and your call.** I would keep saving the confirmed booking
(row 4) synchronous. The caller needs the room to be held before receiving a
successful `Booking` result; otherwise another request could claim the same
slot while the save waits on an event. If the store is unavailable, returning
an error is appropriate because the service cannot truthfully promise the
reservation exists. This is core state, unlike email, receipts, reminders,
dashboard statistics, and the search projection.

### The incident

**The call behind `TICKET.md`.** In `src/service/bookingService.ts`, method
`BookingService.book`, line 94 calls
`this.searchIndex.indexBooking(booking, room, now)`. When the search client
throws `SearchIndexUnavailableError`, `book` does not catch it: the caller gets
an exception instead of a `Booking`, and execution never reaches the store
save on line 95.

---

## Milestone 2: what a second delivery does

### Your prediction (before you run anything)

**What you expect `redeliver` on a refund message to do.** The handler has no
duplicate check, so every delivery of the same refund message will append a
new ledger entry and call the payment provider again. After the original
delivery plus two redeliveries, I predict 3 ledger entries, 3 gateway calls, 3
settlements, and $255.00 returned for one $85.00 refund. The bus log will still
contain one immutable `refund.requested` message with the same id, but its
delivery count will rise from 1 to 3; I expect no dead letters because no call
is armed to fail.

### Before your fix

**The run.** The command printed:

```text
> campus-reservations@1.0.0 demo:replay
> tsx src/demos/replayDemo.ts

booked bk-1001 for 8500 cents
cancelled bk-1001, refund 8500 cents, refund message msg-3

=== after the cancellation (one delivery of the refund message) ===
ledger entries: 1
  led-1 bk-1001 $85.00 (cancelled more than two hours ahead)
gateway calls: 1
gateway settlements: 1
  stl-1 bk-1001 $85.00
money returned in total: $85.00
dead letters: 0

redelivering msg-3

=== after one redelivery ===
ledger entries: 2
  led-1 bk-1001 $85.00 (cancelled more than two hours ahead)
  led-2 bk-1001 $85.00 (cancelled more than two hours ahead)
gateway calls: 2
gateway settlements: 2
  stl-1 bk-1001 $85.00
  stl-2 bk-1001 $85.00
money returned in total: $170.00
dead letters: 0

redelivering msg-3 again

=== after two redeliveries ===
ledger entries: 3
  led-1 bk-1001 $85.00 (cancelled more than two hours ahead)
  led-2 bk-1001 $85.00 (cancelled more than two hours ahead)
  led-3 bk-1001 $85.00 (cancelled more than two hours ahead)
gateway calls: 3
gateway settlements: 3
  stl-1 bk-1001 $85.00
  stl-2 bk-1001 $85.00
  stl-3 bk-1001 $85.00
money returned in total: $255.00
dead letters: 0

=== bus log ===
  msg-1 booking.confirmed deliveries=1
  msg-2 booking.cancelled deliveries=1
  msg-3 refund.requested deliveries=3
```

**Your prediction against the run.** The run agreed with every predicted
observable: 3 ledger entries, 3 gateway calls and settlements, $255.00
returned, no dead letters, and one `msg-3` log record whose delivery count is
3. There was no mismatch.

### Your fix

**The diff.**

```diff
diff --git a/src/handlers/refundLedger.ts b/src/handlers/refundLedger.ts
index 600ffca..1a6e316 100644
--- a/src/handlers/refundLedger.ts
+++ b/src/handlers/refundLedger.ts
@@ -24,6 +24,8 @@ export class RefundLedger implements Subscriber<RefundRequested> {
   readonly name = 'RefundLedger';

   private readonly entries: LedgerEntry[] = [];
+  private readonly entryByMessageId = new Map<string, LedgerEntry>();
+  private readonly completedMessageIds = new Set<string>();
   private readonly gateway: PaymentGateway;

   constructor(gateway: PaymentGateway) {
@@ -31,16 +33,26 @@ export class RefundLedger implements Subscriber<RefundRequested> {
   }

   handle(message: Message<RefundRequested>): void {
+    if (this.completedMessageIds.has(message.id)) {
+      return;
+    }
+
     const event = message.payload;
-    this.entries.push({
-      entryId: `led-${this.entries.length + 1}`,
-      bookingId: event.bookingId,
-      organizer: event.organizer,
-      amountCents: event.amountCents,
-      reason: event.reason,
-      recordedAtMinute: message.publishedAtMinute,
-    });
+    if (!this.entryByMessageId.has(message.id)) {
+      const entry: LedgerEntry = {
+        entryId: `led-${this.entries.length + 1}`,
+        bookingId: event.bookingId,
+        organizer: event.organizer,
+        amountCents: event.amountCents,
+        reason: event.reason,
+        recordedAtMinute: message.publishedAtMinute,
+      };
+      this.entries.push(entry);
+      this.entryByMessageId.set(message.id, entry);
+    }
+
     this.gateway.refund(event.bookingId, event.amountCents);
+    this.completedMessageIds.add(message.id);
   }

   /** Every ledger entry, oldest first. */
```

**What your fix uses to decide what to do on a second delivery, and what it
does not protect.** It keys both the ledger entry and completed-payment marker
by the bus's stable `message.id`: an incomplete delivery retries only the
payment, while a completed delivery does nothing. These markers are only
in-memory and the gateway is not given an idempotency key, so this does not
protect a process restart or a crash after the provider accepts the payment
but before `completedMessageIds.add` runs; it also does not deduplicate two
different message ids requesting the same refund.

### After your fix

**The run.** The fixed command printed:

```text
> campus-reservations@1.0.0 demo:replay
> tsx src/demos/replayDemo.ts

booked bk-1001 for 8500 cents
cancelled bk-1001, refund 8500 cents, refund message msg-3

=== after the cancellation (one delivery of the refund message) ===
ledger entries: 1
  led-1 bk-1001 $85.00 (cancelled more than two hours ahead)
gateway calls: 1
gateway settlements: 1
  stl-1 bk-1001 $85.00
money returned in total: $85.00
dead letters: 0

redelivering msg-3

=== after one redelivery ===
ledger entries: 1
  led-1 bk-1001 $85.00 (cancelled more than two hours ahead)
gateway calls: 1
gateway settlements: 1
  stl-1 bk-1001 $85.00
money returned in total: $85.00
dead letters: 0

redelivering msg-3 again

=== after two redeliveries ===
ledger entries: 1
  led-1 bk-1001 $85.00 (cancelled more than two hours ahead)
gateway calls: 1
gateway settlements: 1
  stl-1 bk-1001 $85.00
money returned in total: $85.00
dead letters: 0

=== bus log ===
  msg-1 booking.confirmed deliveries=1
  msg-2 booking.cancelled deliveries=1
  msg-3 refund.requested deliveries=3
```

**What stays the same across its three states.** After cancellation, one
redelivery, and two redeliveries, the ledger remains at 1 entry, the gateway
at 1 call and 1 settlement, total returned at $85.00, and dead letters at 0.
Only the bus delivery count changes, reaching 3 for `msg-3`.

---

## Milestone 3: a delivery that stops halfway

### The failure

**The run.** `npm run demo:failure` left:

```text
=== after the failed delivery ===
ledger entries: 1
  led-1 bk-1001 $85.00 (cancelled more than two hours ahead)
gateway calls: 1
gateway settlements: 0
money returned in total: $0.00
dead letters: 1
  msg-3 refund.requested attempt 1 in RefundLedger: payment provider did not answer on call 1 for bk-1001
    failedAtMinute: 480
    payload: {"bookingId":"bk-1001","organizer":"H. Ahmad","organizerEmail":"hahmad@example.edu","amountCents":8500,"reason":"cancelled more than two hours ahead","requestedAtMinute":480}
```

### Your recovery

**What, if anything, the failed delivery left undone.** It recorded the single
$85.00 ledger entry, but the gateway threw before settlement, so the organizer
had received $0.00. The cancellation itself still returned successfully and
the booking stayed cancelled because the bus catches subscriber errors.

**The option you took and its evidence.** I redelivered
`cancellation.refundMessageId` after the simulated provider recovered. The
handler reused the existing ledger entry, retried the incomplete gateway step,
and marked the message complete:

```text
=== after redelivery ===
ledger entries: 1
  led-1 bk-1001 $85.00 (cancelled more than two hours ahead)
gateway calls: 2
gateway settlements: 1
  stl-1 bk-1001 $85.00
money returned in total: $85.00
dead letters: 1
  msg-3 refund.requested attempt 1 in RefundLedger: payment provider did not answer on call 1 for bk-1001
    failedAtMinute: 480
    payload: {"bookingId":"bk-1001","organizer":"H. Ahmad","organizerEmail":"hahmad@example.edu","amountCents":8500,"reason":"cancelled more than two hours ahead","requestedAtMinute":480}

=== bus log ===
  msg-1 booking.confirmed deliveries=1
  msg-2 booking.cancelled deliveries=1
  msg-3 refund.requested deliveries=2
```

The old dead letter remains because it is a failure log, not a retry queue; its
presence after recovery does not mean attempt 2 failed.

### The other option

**What the option you did not take would have cost.** Manual triage would make
an operator inspect `message.id` (the replay key), `message.type` and
`subscriberName` (which workflow and component failed), `payload.bookingId`
and `payload.amountCents` (what to reconcile/pay), `reason` (why it failed),
`attempt` (how many delivery attempts had reached this failure), and
`failedAtMinute` (when). The operator would have to verify the provider had
not settled the refund before paying or replaying it, delaying the organizer's
money until a human acts; it is safe only with reliable provider reconciliation
and an idempotent manual payment/replay procedure. Automatic redelivery was
faster here, but in production it would also require durable idempotency state
and a provider-side idempotency key to close the crash window described in
milestone 2.
