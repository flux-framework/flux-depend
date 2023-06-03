// use log::{error, info, warn};
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;

struct Job {
    jobid: i64,
    user: u32,
    dependencies: Vec<Dependency>,
    ancestors: HashSet<i64>,   // jobs this job depends on
    descendants: HashSet<i64>, // jobs that depend on this job
}

impl Job {
    pub fn new(jobid: i64, user: u32, dependencies: Vec<Dependency>) -> Self {
        Self {
            jobid: jobid,
            user: user,
            dependencies: dependencies,
            ancestors: HashSet::new(),
            descendants: HashSet::new(),
        }
    }
}

#[derive(Debug, PartialEq)]
enum StateError {
    InvalidJobID,
    DuplicateJobID,
    MissingDescendant,
    InvalidEvent,
    InvalidPermission,
}

type SymbolMap = HashMap<String, HashSet<i64>>;

struct State {
    instance_owner: u32,
    jobs: HashMap<i64, Job>,
    /// Global symbol lookup table used when a new job with an `in` dependency
    /// is ingested.  We can look for that dependency in this table and find all
    /// relevant jobs to add as ancestors of the new job.
    global_symbol_map: SymbolMap,
    user_symbol_map: HashMap<u32, SymbolMap>,
}

impl State {
    pub fn new(instance_owner: u32) -> Self {
        Self {
            instance_owner: instance_owner,
            jobs: HashMap::new(),
            global_symbol_map: HashMap::new(),
            user_symbol_map: HashMap::new(),
        }
    }

    pub fn add_in_dependency(
        &mut self,
        in_job: &mut Job,
        dependency: &Dependency,
    ) -> Result<(), StateError> {
        /*! Look for previously submitted jobs whose `out` symbol matches the
         * `in` symbol of the provided job.  For each matching job: add the new
         * job as a descendants of the previously submitted job and add the
         * previously submitted job as an ancestor of the new job.  If no
         * matches are found, do nothing (i.e., the job should be free to be
         * scheduled immediately)
         */
        let symbol = &dependency.value;
        let out_jobs = match self.get_symbol_map(in_job.user, dependency).get(symbol) {
            Some(out_jobs) => {
                out_jobs.clone() // satisfy the borrow-checker, release the &mut to self
            }
            None => return Ok(()),
        };
        for out_jobid in out_jobs.iter() {
            // TODO: does this need to be mut?
            let out_job: &mut Job = match self.jobs.get_mut(out_jobid) {
                Some(x) => x,
                None => {
                    eprintln!("Failed to find {} in the jobs map", out_jobid);
                    return Err(StateError::InvalidJobID);
                }
            };
            in_job.ancestors.insert(out_job.jobid);
            out_job.descendants.insert(in_job.jobid);
        }
        Ok(())
    }

    pub fn add_out_dependency(
        &mut self,
        out_job: &Job,
        dependency: &Dependency,
    ) -> Result<(), StateError> {
        //! Add the new job to the appropriate key in the `jobs_out_symbol`
        //! lookup table, where the "appropriate key" is the `out` symbol.
        if (dependency.scope == DependencyScope::Global) && (out_job.user != self.instance_owner) {
            return Err(StateError::InvalidPermission);
        }

        let symbol_map = self.get_symbol_map(out_job.user, dependency);
        let symbol = &dependency.value;
        let out_jobs = symbol_map
            .entry(symbol.to_string())
            .or_insert(HashSet::new());
        out_jobs.insert(out_job.jobid);

        Ok(())
    }

    pub fn rollback_job_add(&mut self, _job: &Job) {}

    fn get_symbol_map(&mut self, user: u32, dependency: &Dependency) -> &mut SymbolMap {
        return match dependency.scope {
            DependencyScope::Global => &mut self.global_symbol_map,
            DependencyScope::User => self.user_symbol_map.entry(user).or_insert(HashMap::new()),
        };
    }

    pub fn add_job(&mut self, mut job: Job) -> Result<(), StateError> {
        /*! Add a new job into the dependency graph, considering all of its
         * dependencies.  For each dependency of the new job, InOut
         * dependencies are broken down into an `In` insertion followed by an
         * an `Out` insertion.
         */
        if self.jobs.contains_key(&job.jobid) {
            return Err(StateError::DuplicateJobID);
        }

        for dependency in job.dependencies.clone().iter() {
            let result = match dependency.dep_type {
                DependencyType::In => self.add_in_dependency(&mut job, &dependency),
                DependencyType::Out => self.add_out_dependency(&mut job, &dependency),
                DependencyType::InOut => {
                    let in_ret = self.add_in_dependency(&mut job, &dependency);
                    if in_ret.is_err() {
                        in_ret
                    } else {
                        self.add_out_dependency(&mut job, &dependency)
                    }
                }
            };
            match result {
                Ok(()) => {}
                Err(e) => {
                    self.rollback_job_add(&job);
                    return Err(e);
                }
            }
        }

        self.jobs.insert(job.jobid, job);
        Ok(())
    }

    fn remove_job_from_dependents(&mut self, jobid: i64) -> Result<HashSet<i64>, StateError> {
        let job = match self.jobs.get_mut(&jobid) {
            Some(x) => x,
            None => return Err(StateError::InvalidJobID),
        };

        let mut error_occurred: bool = false;
        let error = StateError::MissingDescendant;
        let mut ret = HashSet::new();

        for descendant_id in job.descendants.clone().iter() {
            let descendant_job = match self.jobs.get_mut(descendant_id) {
                Some(x) => x,
                None => {
                    eprintln!("Descendant Job ID ({}) not found", descendant_id);
                    error_occurred = true;
                    continue;
                }
            };
            if !descendant_job.ancestors.remove(&jobid) {
                eprintln!("WARN: Job ID not found in descendant's ancestor list");
            }
            if descendant_job.ancestors.len() == 0 {
                ret.insert(descendant_job.jobid);
            }
        }

        if error_occurred {
            return Err(error);
        }

        Ok(ret)
    }

    fn remove_job_from_state_maps(&mut self, jobid: i64) -> Result<(), StateError> {
        let job = match self.jobs.get_mut(&jobid) {
            Some(x) => x,
            None => return Err(StateError::InvalidJobID),
        };

        let out_dependencies: Vec<&Dependency> = job
            .dependencies
            .iter()
            .filter(|x| {
                (x.dep_type == DependencyType::Out) || (x.dep_type == DependencyType::InOut)
            })
            .collect();

        // TODO: implement fluid support
        // delete from matching 'out' labels in global/user scope
        for dependency in out_dependencies
            .iter()
            .filter(|x| x.scope == DependencyScope::Global)
        {
            let relevant_map = self
                .global_symbol_map
                .get_mut(&dependency.value)
                .expect("Dependency value missing in global map");
            if relevant_map.remove(&jobid) == false {
                eprintln!(
                    "Failed to remove {}'s {} out dependency from the global scope",
                    jobid, dependency.value
                );
            }
            if relevant_map.len() == 0 {
                self.global_symbol_map.remove(&dependency.value);
            }
        }

        match self.user_symbol_map.get_mut(&job.user) {
            Some(user_map) => {
                for dependency in out_dependencies
                    .iter()
                    .filter(|x| x.scope == DependencyScope::User)
                {
                    let relevant_map = user_map
                        .get_mut(&dependency.value)
                        .expect("Dependency value missing in user map");
                    if relevant_map .remove(&jobid)
                        == false
                    {
                        eprintln!(
                            "Failed to remove {}'s {} out dependency from user {}'s scope",
                            jobid, dependency.value, job.user
                        );
                    }
                    if relevant_map.len() == 0 {
                        user_map.remove(&dependency.value);
                    }
                }
                if user_map.len() == 0 {
                    self.user_symbol_map.remove(&job.user);
                }
            }
            None => {}
        }

        // match job.scope
        self.jobs.remove(&jobid);

        Ok(())
    }

    fn submit_event(&mut self, jobid: i64) -> Result<HashSet<i64>, StateError> {
        let job = match self.jobs.get_mut(&jobid) {
            Some(x) => x,
            None => return Err(StateError::InvalidJobID),
        };

        let mut ret = HashSet::new();
        if job.ancestors.len() == 0 {
            ret.insert(job.jobid);
        }
        Ok(ret)
    }

    pub fn job_event(&mut self, jobid: i64, event: String) -> Result<HashSet<i64>, StateError> {
        /*! Given a specific job and it's event, calculate the effects of this
         * event on other jobs.  Specifically, calculate which jobs are now
         * free to run. For example, if a job completes, determine which jobs
         * are now free to run given the completion of their dependency.
         */
        match event.as_str() {
            "submit" => self.submit_event(jobid),
            "depend" => Ok(HashSet::new()),
            "alloc" => Ok(HashSet::new()),
            "finish" | "cancel" => {
                let ret = self
                    .remove_job_from_dependents(jobid)
                    .map_err(|err| return err);
                let _ret2 = self
                    .remove_job_from_state_maps(jobid)
                    .map_err(|err| return err);

                ret
            }
            _ => return Err(StateError::InvalidEvent),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum DependencyType {
    In,
    Out,
    InOut,
}

#[derive(Debug, Clone, PartialEq)]
enum DependencyScope {
    User,
    Global,
}

#[derive(Debug, Clone, PartialEq)]
enum DependencyScheme {
    String,
    Fluid,
}

#[derive(Debug, Clone, PartialEq)]
struct Dependency {
    dep_type: DependencyType,
    scope: DependencyScope,
    scheme: DependencyScheme,
    value: String,
}

impl Dependency {
    pub fn new(
        dep_type: DependencyType,
        scope: DependencyScope,
        scheme: DependencyScheme,
        value: String,
    ) -> Self {
        Self {
            dep_type: dep_type,
            scope: scope,
            scheme: scheme,
            value: value,
        }
    }

    pub fn new_global_string(dep_type: DependencyType, value: String) -> Self {
        Self {
            dep_type: dep_type,
            scope: DependencyScope::Global,
            scheme: DependencyScheme::String,
            value: value,
        }
    }
}

fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_noop(actual: Result<HashSet<i64>, StateError>) {
        assert!(actual.is_ok());
        assert_eq!(actual.unwrap().len(), 0)
    }

    fn assert_jobs_eq(actual: Result<HashSet<i64>, StateError>, expected: &Vec<i64>) {
        assert!(actual.is_ok());
        assert_eq!(
            actual.unwrap(),
            HashSet::from_iter(expected.iter().cloned())
        );
    }

    fn assert_err_eq<T: std::fmt::Debug>(actual: Result<T, StateError>, expected: StateError) {
        // replace with `contains_err` once it is stable
        assert!(actual.is_err());
        assert_eq!(actual.unwrap_err(), expected,);
    }

    #[test]
    fn owner_job_chain() {
        let scheme = DependencyScheme::String;
        for scope in vec![DependencyScope::Global, DependencyScope::User] {
            let mut state = State::new(1);

            state
                .add_job(Job::new(
                    1,
                    1,
                    vec![Dependency::new(
                        DependencyType::Out,
                        scope.clone(),
                        scheme.clone(),
                        "foo".to_string(),
                    )],
                ))
                .expect("Add job failed");
            state
                .add_job(Job::new(
                    2,
                    1,
                    vec![Dependency::new(
                        DependencyType::InOut,
                        scope.clone(),
                        scheme.clone(),
                        "foo".to_string(),
                    )],
                ))
                .expect("Add job failed");
            state
                .add_job(Job::new(
                    3,
                    1,
                    vec![Dependency::new(
                        DependencyType::In,
                        scope.clone(),
                        scheme.clone(),
                        "foo".to_string(),
                    )],
                ))
                .expect("Add job failed");

            // Submit all the things!
            let out = state.job_event(1, "submit".to_string());
            assert_jobs_eq(out, &vec![1]);
            for jobid in vec![2, 3].iter() {
                let out = state.job_event(*jobid, "submit".to_string());
                assert_noop(out);
            }

            for jobid in vec![1, 2, 3].iter() {
                let out = state.job_event(*jobid, "depend".to_string());
                assert_noop(out);
                let out = state.job_event(*jobid, "alloc".to_string());
                assert_noop(out);
                let out = state.job_event(*jobid, "finish".to_string());
                if *jobid < 3 {
                    assert_jobs_eq(out, &vec![jobid + 1]);
                } else {
                    assert_noop(out);
                }
            }
        }
    }

    #[test]
    fn job_fan_out() {
        let mut state = State::new(1);
        state
            .add_job(Job::new(
                1,
                1,
                vec![Dependency::new_global_string(
                    DependencyType::Out,
                    "foo".to_string(),
                )],
            ))
            .expect("Add job failed");
        for jobid in vec![2, 3, 4].iter() {
            state
                .add_job(Job::new(
                    *jobid,
                    1,
                    vec![
                        Dependency::new_global_string(DependencyType::In, "foo".to_string()),
                        Dependency::new_global_string(DependencyType::Out, "bar".to_string()),
                    ],
                ))
                .expect("Add job failed");
        }
        state
            .add_job(Job::new(
                5,
                1,
                vec![Dependency::new_global_string(
                    DependencyType::In,
                    "bar".to_string(),
                )],
            ))
            .expect("Add job failed");

        // Submit all the things!
        assert_jobs_eq(state.job_event(1, "submit".to_string()), &vec![1]);
        for jobid in vec![2, 3, 4, 5].iter() {
            let out = state.job_event(*jobid, "submit".to_string());
            assert_noop(out);
        }

        // Run and complete initial pre-process job
        assert_noop(state.job_event(1, "depend".to_string()));
        assert_noop(state.job_event(1, "alloc".to_string()));
        assert_jobs_eq(state.job_event(1, "finish".to_string()), &vec![2, 3, 4]);

        // Run and complete fan-out
        for jobid in vec![2, 3, 4].iter() {
            let out = state.job_event(*jobid, "depend".to_string());
            assert_noop(out);
            let out = state.job_event(*jobid, "alloc".to_string());
            assert_noop(out);
            let out = state.job_event(*jobid, "finish".to_string());
            if *jobid < 4 {
                assert_noop(out);
            } else {
                assert_jobs_eq(out, &vec![5]);
            }
        }

        // Run and complete postprocess job
        assert_noop(state.job_event(5, "depend".to_string()));
        assert_noop(state.job_event(5, "alloc".to_string()));
        assert_noop(state.job_event(5, "finish".to_string()));
    }

    #[test]
    fn nonexistent_in() {
        //! A job with an 'in' dependency that does not match an 'out' of a
        //! currently queued/running job can be immediately scheduled.
        let mut state = State::new(1);
        state
            .add_job(Job::new(
                1,
                1,
                vec![Dependency::new_global_string(
                    DependencyType::In,
                    "foo".to_string(),
                )],
            ))
            .expect("Add job failed");
        assert_jobs_eq(state.job_event(1, "submit".to_string()), &vec![1]);
    }

    #[test]
    fn invalid_jobid() {
        //! Test that an event on an unknown/invalid returns an error
        let mut state = State::new(1);
        let out = state.job_event(1, "submit".to_string());
        assert_err_eq(out, StateError::InvalidJobID);
        state
            .add_job(Job::new(
                1,
                1,
                vec![Dependency::new_global_string(
                    DependencyType::In,
                    "foo".to_string(),
                )],
            ))
            .expect("Add job failed");
        let out = state.job_event(1, "foobar".to_string());
        assert_err_eq(out, StateError::InvalidEvent);
    }

    #[test]
    fn empty_dependencies() {
        //! Test that a job without any dependencies works
        let mut state = State::new(1);
        state
            .add_job(Job::new(1, 1, vec![]))
            .expect("Add job failed");
        assert_jobs_eq(state.job_event(1, "submit".to_string()), &vec![1]);
    }

    #[test]
    fn invalid_global_out_dep() {
        let mut state = State::new(1);
        let out = state.add_job(Job::new(
            1,
            2,
            vec![Dependency::new_global_string(
                DependencyType::Out,
                "foo".to_string(),
            )],
        ));
        assert_err_eq(out, StateError::InvalidPermission);
    }

    #[test]
    fn test_user_global_separation() {
        let mut state = State::new(1);
        state
            .add_job(Job::new(
                1,
                1,
                vec![Dependency::new(
                    DependencyType::Out,
                    DependencyScope::Global,
                    DependencyScheme::String,
                    "foo".to_string(),
                )],
            ))
            .expect("Add job failed");
        state
            .add_job(Job::new(
                2,
                2,
                vec![Dependency::new(
                    DependencyType::In,
                    DependencyScope::User,
                    DependencyScheme::String,
                    "foo".to_string(),
                )],
            ))
            .expect("Add job failed");
        // Both jobs should be able to run right away.  The user In dependency
        // should NOT match with the global Out dependency
        for jobid in vec![1, 2].iter() {
            let out = state.job_event(*jobid, "submit".to_string());
            assert_jobs_eq(out, &vec![*jobid]);
        }
    }

    #[test]
    fn test_user_global_interleave() {
        let mut state = State::new(1);
        state
            .add_job(Job::new(
                1,
                1,
                vec![Dependency::new(
                    DependencyType::Out,
                    DependencyScope::Global,
                    DependencyScheme::String,
                    "foo".to_string(),
                )],
            ))
            .expect("Add job failed");
        state
            .add_job(Job::new(
                2,
                2,
                vec![
                    Dependency::new(
                        DependencyType::In,
                        DependencyScope::Global,
                        DependencyScheme::String,
                        "foo".to_string(),
                    ),
                    Dependency::new(
                        DependencyType::Out,
                        DependencyScope::User,
                        DependencyScheme::String,
                        "bar".to_string(),
                    ),
                ],
            ))
            .expect("Add job failed");
        state
            .add_job(Job::new(
                3,
                2,
                vec![Dependency::new(
                    DependencyType::In,
                    DependencyScope::User,
                    DependencyScheme::String,
                    "bar".to_string(),
                )],
            ))
            .expect("Add job failed");
        assert_jobs_eq(state.job_event(1, "submit".to_string()), &vec![1]);
        for jobid in vec![2, 3].iter() {
            let out = state.job_event(*jobid, "submit".to_string());
            assert_noop(out);
        }
        assert_noop(state.job_event(1, "depend".to_string()));
        assert_noop(state.job_event(1, "alloc".to_string()));
        assert_jobs_eq(state.job_event(1, "finish".to_string()), &vec![2]);
        assert_noop(state.job_event(2, "depend".to_string()));
        assert_noop(state.job_event(2, "alloc".to_string()));
        assert_jobs_eq(state.job_event(2, "finish".to_string()), &vec![3]);
    }

    #[test]
    fn test_duplicate_id() {
        let mut state = State::new(1);
        state
            .add_job(Job::new(
                1,
                1,
                vec![Dependency::new(
                    DependencyType::Out,
                    DependencyScope::Global,
                    DependencyScheme::String,
                    "foo".to_string(),
                )],
            ))
            .expect("Add job failed");
        assert_err_eq(
            state.add_job(Job::new(
                1,
                1,
                vec![Dependency::new(
                    DependencyType::Out,
                    DependencyScope::Global,
                    DependencyScheme::String,
                    "foo".to_string(),
                )],
            )),
            StateError::DuplicateJobID,
        );
        assert_err_eq(
            state.add_job(Job::new(
                1,
                2,
                vec![Dependency::new(
                    DependencyType::Out,
                    DependencyScope::Global,
                    DependencyScheme::String,
                    "foo".to_string(),
                )],
            )),
            StateError::DuplicateJobID,
        );
    }

    #[test]
    fn test_cleanup_after_cancel() {
        let mut state = State::new(2);
        state
            .add_job(Job::new(
                1,
                2,
                vec![Dependency::new(
                    DependencyType::Out,
                    DependencyScope::Global,
                    DependencyScheme::String,
                    "foo".to_string(),
                )],
            ))
            .expect("Add job failed");
        assert_jobs_eq(state.job_event(1, "submit".to_string()), &vec![1]);
        assert_noop(state.job_event(1, "depend".to_string()));
        assert_noop(state.job_event(1, "cancel".to_string()));
        assert_err_eq(
            state.job_event(1, "finish".to_string()),
            StateError::InvalidJobID,
        );
        assert!(state.global_symbol_map.get("foo").is_none());
        assert!(state.jobs.get(&1).is_none());

        state
            .add_job(Job::new(
                2,
                1,
                vec![Dependency::new(
                    DependencyType::Out,
                    DependencyScope::User,
                    DependencyScheme::String,
                    "foo".to_string(),
                )],
            ))
            .expect("Add job failed");
        assert_jobs_eq(state.job_event(2, "submit".to_string()), &vec![2]);
        assert!(
            state
                .user_symbol_map
                .get(&1)
                .expect("User not in user symbol map")
                .get("foo")
                .unwrap()
                .len()
                == 1
        );
        assert_noop(state.job_event(2, "depend".to_string()));
        assert_noop(state.job_event(2, "cancel".to_string()));
        assert!(state.jobs.get(&2).is_none());
        assert!(state.user_symbol_map.get(&1).is_none());

        assert!(state.jobs.len() == 0);
        assert!(state.global_symbol_map.len() == 0);
        assert!(state.user_symbol_map.len() == 0);
    }

    #[test]
    fn test_cancel_job() {
        let mut state = State::new(1);
        state
            .add_job(Job::new(
                1,
                1,
                vec![Dependency::new(
                    DependencyType::Out,
                    DependencyScope::Global,
                    DependencyScheme::String,
                    "foo".to_string(),
                )],
            ))
            .expect("Add job failed");
        state
            .add_job(Job::new(
                2,
                1,
                vec![Dependency::new(
                    DependencyType::In,
                    DependencyScope::Global,
                    DependencyScheme::String,
                    "foo".to_string(),
                )],
            ))
            .expect("Add job failed");
        assert_jobs_eq(state.job_event(1, "submit".to_string()), &vec![1]);
        assert_noop(state.job_event(1, "depend".to_string()));
        assert_noop(state.job_event(2, "submit".to_string()));
        assert_jobs_eq(state.job_event(1, "cancel".to_string()), &vec![2]);
        assert_noop(state.job_event(2, "depend".to_string()));
        state
            .add_job(Job::new(
                3,
                1,
                vec![Dependency::new(
                    DependencyType::In,
                    DependencyScope::Global,
                    DependencyScheme::String,
                    "foo".to_string(),
                )],
            ))
            .expect("Add job failed");
        assert_jobs_eq(state.job_event(3, "submit".to_string()), &vec![3]);
    }
}
