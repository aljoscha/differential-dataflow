extern crate differential_dataflow;
extern crate rand;
extern crate timely;

use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::trace::{BatchReader, Cursor, TraceReader};
use timely::dataflow::operators::Probe;

use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeByKey;
use timely::progress::Antichain;

fn main() {
    let num_rounds = 10;
    let num_keys = 1_000_000;
    let num_keys_last_round = 1_000_000;

    timely::execute_from_args(std::env::args(), move |worker| {
        let index = worker.index();
        let peers = worker.peers();

        println!("worker {index}/{peers}");

        let mut probe = timely::dataflow::operators::probe::Handle::new();

        let (mut input, mut trace) = worker.dataflow::<u64, _, _>(|scope| {
            let (input_handle, input) = scope.new_collection::<_, i64>();

            let arranged = input.arrange_by_key();

            arranged.stream.probe_with(&mut probe);

            (input_handle, arranged.trace)
        });

        for round in 0..num_rounds {
            println!("round {round}");

            if index == 0 {
                // Last round emits one order of magnitude more keys
                let (num_retractions, num_additions) = if round == 0 {
                    // First round has no retractions.
                    (0, num_keys)
                } else if round == num_rounds - 1 {
                    (num_keys, num_keys_last_round)
                } else {
                    (num_keys, num_keys)
                };

                // Retract updates from previous round.
                for key in 0..num_retractions {
                    input.update((key, round - 1), -1);
                }

                // Add updates for new round.
                for key in 0..num_additions {
                    input.update((key, round), 1);
                }
            }

            input.advance_to(round + 1);
            input.flush();
            worker.step_while(|| probe.less_than(input.time()));

            let mut upper = Antichain::new();
            trace.read_upper(&mut upper);
            println!("trace upper: {:?}", upper);

            let mut num_batches = 0;
            let mut num_entries = 0;

            trace.set_logical_compaction(Antichain::from_elem(round + 1).borrow());
            trace.set_physical_compaction(Antichain::from_elem(round + 1).borrow());

            let mut updates = Vec::new();
            trace.map_batches(|batch| {
                num_batches += 1;
                let mut num_entries_in_batch = 0;
                let mut cursor = batch.cursor();
                while let Some(key) = cursor.get_key(&batch) {
                    while let Some(val) = cursor.get_val(&batch) {
                        cursor.map_times(&batch, |time, diff| {
                            updates.push(((*key, *val), *time, *diff));
                            num_entries_in_batch += 1;
                        });
                        cursor.step_val(&batch);
                    }
                    cursor.step_key(&batch);
                }
                println!(
                    "batch: {:?}, entries: {num_entries_in_batch}",
                    batch.description()
                );

                num_entries += num_entries_in_batch;
            });

            consolidate_updates(&mut updates);

            println!("num_batches: {num_batches}, num_entries: {num_entries}");
            println!("consolidated updates.len: {}", updates.len());
            println!("---\n");
        }
    })
    .unwrap();
}
