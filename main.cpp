#include <iostream>
#include <thread>
#include <chrono>
#include "taskflow/taskflow.hpp"

using namespace std::chrono_literals;

template<typename ...Args>
auto log(std::size_t worker_id, const Args&... args)
{
    std::cerr << "[worker_id:" + std::to_string(worker_id) + "] " + ((std::string(args) + " ") + ...) + "\n";
}

struct ExecutionObserver : public tf::ObserverInterface
{

    void set_up(size_t num_workers) override
    {
        std::cerr << "Setting up observer with " + std::to_string(num_workers) + " number of workers\n";
    }

    void on_entry(tf::WorkerView w, tf::TaskView tv) override
    {
        log(w.id(), "on_entry, task name:", tv.name(), "task type:", tf::to_string(tv.type()));
    }

    void on_exit(tf::WorkerView w, tf::TaskView tv) override
    {
        log(w.id(), "on_exit, task name:", tv.name(), "task type:", tf::to_string(tv.type()));
    }
};

struct Sema
{
    Sema(std::size_t count, std::string name) : s(count), name(std::move(name)){}
    tf::Semaphore s;
    std::string name;
};


namespace s2
{
    auto makeS2Flow(tf::Taskflow& tf, const std::string& name, Sema& sema, std::chrono::milliseconds delay) -> auto
{
    auto t = tf.emplace([&sema, name, delay](tf::Runtime& rt){
        log(rt.worker().id(), name, "acquiring", sema.name);
        rt.acquire(sema.s); 
        log(rt.worker().id(), name, "starting work, acquired", sema.name);
        std::this_thread::sleep_for(delay); 
        log(rt.worker().id(), name, "work done , releasing", sema.name);
        rt.release(sema.s); 
        log(rt.worker().id(), name, "released", sema.name);
        });
    t.name(name + "_acquire_and_release_" + sema.name);
    return t;
}

auto makeS2FlowSteal(tf::Taskflow& tf, const std::string& name, Sema& sema, Sema& semaForSteal, std::chrono::milliseconds delay) -> void
{
    auto f1a = tf.emplace([&sema, name](tf::Runtime& rt){
        log(rt.worker().id(), name, "acquiring", sema.name);
        rt.acquire(sema.s); 
        log(rt.worker().id(), name, "acquired", sema.name);
        });

    auto f1b = tf.emplace([&semaForSteal, name, delay](tf::Runtime& rt){
        log(rt.worker().id(), name, "starting work and acquiring", semaForSteal.name);
        rt.acquire(semaForSteal.s);
        std::this_thread::sleep_for(delay); 
        rt.release(semaForSteal.s);
        log(rt.worker().id(), name, "work done and released", semaForSteal.name);
        });

    auto f1c = tf.emplace([&sema, name](tf::Runtime& rt){
        log(rt.worker().id(), name, "releasing", sema.name);
        rt.release(sema.s); 
        log(rt.worker().id(), name, "released", sema.name);
        });

    f1a.precede(f1b);
    f1b.precede(f1c);
    f1a.name(name + "_aquire_" + sema.name);
    f1b.name(name + "_acquire_and_release_" + semaForSteal.name + "_under_" + sema.name);
    f1c.name(name + "_release_" + sema.name);
}

auto makeS2SplitFlowSteal(tf::Taskflow& tf, const std::string& name, Sema& sema, Sema& semaForSteal, std::chrono::milliseconds delay) -> void
{
    auto f1a = tf.emplace([&sema, name](tf::Runtime& rt){
        log(rt.worker().id(), name, "acquiring", sema.name);
        rt.acquire(sema.s); 
        log(rt.worker().id(), name, "acquired", sema.name);
        });

    auto f1b = tf.emplace([&semaForSteal, name, delay](tf::Runtime& rt){
        log(rt.worker().id(), name, "starting work and acquiring", semaForSteal.name);
        rt.acquire(semaForSteal.s);
        std::this_thread::sleep_for(delay); 
        });

    auto f1c = tf.emplace([&semaForSteal, name, delay](tf::Runtime& rt){
        rt.release(semaForSteal.s);
        log(rt.worker().id(), name, "work done and released", semaForSteal.name);
        });

    auto f1d = tf.emplace([&sema, name](tf::Runtime& rt){
        log(rt.worker().id(), name, "releasing", sema.name);
        rt.release(sema.s); 
        log(rt.worker().id(), name, "released", sema.name);
        });

    f1a.precede(f1b);
    f1b.precede(f1c);
    f1c.precede(f1d);
    f1a.name(name + "_aquire_" + sema.name);
    f1b.name(name + "_acquire_" + semaForSteal.name + "_under_" + sema.name);
    f1c.name(name + "_release_" + semaForSteal.name + "_under_" + sema.name);
    f1d.name(name + "_release_" + sema.name);
}

auto s2() -> void 
{
    auto sema1 = Sema(1, "sema1");
    auto sema2 = Sema(1, "sema2");

    auto tftop = tf::Taskflow{};
    tftop.name("tftop");

    makeS2Flow(tftop, "f1", sema1, 300ms); // block sema for long
    // makeS2FlowSteal(tf, "f2", sema2, sema1, 100ms); // will steal tasks before doing work
    makeS2SplitFlowSteal(tftop, "f2", sema2, sema1, 100ms); // will steal tasks before doing work
    makeS2Flow(tftop, "f3", sema2, 100ms);

    {
        auto d = std::ofstream("dump-s2.dot");
        tftop.dump(d);
    }

    auto executor = tf::Executor(2);
    executor.make_observer<ExecutionObserver>();
    auto fut = executor.run(tftop,[]{std::cerr << "finish\n";});
    fut.get();
}
}  // namespace s2




namespace s6
{
auto makeWorkFlow(tf::Taskflow& tf, const std::string& name, std::chrono::milliseconds delay) -> auto
{
    auto t = tf.emplace([name, delay](tf::Runtime& rt){
        log(rt.worker().id(), name, "starting work");
        std::this_thread::sleep_for(delay); 
        log(rt.worker().id(), name, "work done");
        });
    t.name(name);
    return t;
}

auto makeInnerWorkFlow(tf::Taskflow& tf, const std::string& name) -> auto
{
    auto t1 = makeWorkFlow(tf, name + "_f1", 40ms);
    auto t2 = makeWorkFlow(tf, name + "_f2", 20ms);
    auto t3 = makeWorkFlow(tf, name + "_f3", 1ms);
    t1.precede(t3);
    t2.precede(t3);
}

auto makeSemaFlow(tf::Taskflow& f1, const std::string& name, Sema& sema, std::chrono::milliseconds delay) -> auto
{
    auto f1a = f1.emplace([&sema, name](tf::Runtime& rt){
        log(rt.worker().id(), name, "acquiring", sema.name);
        rt.acquire(sema.s); 
        log(rt.worker().id(), name, "acquired", sema.name);
        });

    auto f1b = f1.emplace([name, delay](tf::Runtime& rt){
        log(rt.worker().id(), name, "starting work");
        std::this_thread::sleep_for(delay); 
        log(rt.worker().id(), name, "work done");
        });

    auto f1c = f1.emplace([&sema, name](tf::Runtime& rt){
        log(rt.worker().id(), name, "releasing", sema.name);
        rt.release(sema.s); 
        log(rt.worker().id(), name, "released", sema.name);
        });

    f1a.precede(f1b);
    f1b.precede(f1c);
    f1a.name(name + "_aquire_" + sema.name);
    f1b.name(name + "_work_under_" + sema.name);
    f1c.name(name + "_release_" + sema.name);
    return f1a;
}

auto makeSemaFlowWithModule(
    tf::Taskflow& f1, tf::Taskflow& sub, const std::string& name, Sema& sema) -> auto
{
    auto f1a = f1.emplace([&sema, name](tf::Runtime& rt){
        log(rt.worker().id(), name, "acquiring", sema.name);
        rt.acquire(sema.s); 
        log(rt.worker().id(), name, "acquired", sema.name);
        });

    makeInnerWorkFlow(sub, "mod");
    auto f1b = f1.composed_of(sub);

    auto f1c = f1.emplace([&sema, name](tf::Runtime& rt){
        log(rt.worker().id(), name, "releasing", sema.name);
        rt.release(sema.s); 
        log(rt.worker().id(), name, "released", sema.name);
        });

    f1a.precede(f1b);
    f1b.precede(f1c);
    f1a.name(name + "_aquire_" + sema.name);
    f1b.name(name + "_work_under_" + sema.name);
    f1c.name(name + "_release_" + sema.name);
    return f1a;
}


auto s() -> void 
{
    auto sema1 = Sema(1, "sema1");

    auto ftop = tf::Taskflow{};
    ftop.name("ftop");

    auto sub1 = tf::Taskflow{};
    sub1.name("sub1");
    makeSemaFlowWithModule(ftop, sub1, "f1", sema1);
    auto f2 = makeWorkFlow(ftop, "f2", 20ms);
    auto f3 = makeWorkFlow(ftop, "f3", 15ms);
    auto f4 = makeWorkFlow(ftop, "f4", 15ms);

    auto f5 = makeSemaFlow(ftop, "f5", sema1, 5ms);

    f2.precede(f3);
    f2.precede(f4);
    f3.precede(f5);

    {
        auto d = std::ofstream("dump-s6.dot");
        ftop.dump(d);
    }

    auto executor = tf::Executor(3);
    executor.make_observer<ExecutionObserver>();
    auto fut = executor.run(ftop,[]{std::cerr << "finish\n";});
    fut.get();
}
}  // namespace s6




namespace time1
{

using Clock = std::chrono::high_resolution_clock;

template<typename Duration>
auto unit() -> std::string_view
{
    if constexpr (std::is_same_v<Duration, std::chrono::nanoseconds>)
        return "ns";
    else 
    if constexpr (std::is_same_v<Duration, std::chrono::microseconds>)
        return "us";
    else 
    if constexpr (std::is_same_v<Duration, std::chrono::milliseconds>)
        return "ms";
    else 
    if constexpr (std::is_same_v<Duration, std::chrono::seconds>)
        return "s";
    else
        return "unknown unit";
}

auto to_string(Clock::duration duration) -> std::string
{
    return std::to_string(duration.count()).append(unit<Clock::duration>());
}

auto makeWorkFlow(tf::Taskflow& tf, const std::string& name, std::chrono::milliseconds delay) -> auto
{
    auto t = tf.emplace([name, delay](tf::Runtime& rt){
        auto start_time = Clock::now();
        std::this_thread::sleep_for(delay);
        auto end_time = Clock::now();
        log(rt.worker().id(), name, "work done", "elapsed", to_string(end_time-start_time));
        });
    t.name(name);
    return t;
}

auto makeInnerWorkFlow(tf::Taskflow& tf, const std::string& name) -> auto
{
    auto t1 = makeWorkFlow(tf, name + "_f1", 40ms);
    auto t2 = makeWorkFlow(tf, name + "_f2", 20ms);
    auto t3 = makeWorkFlow(tf, name + "_f3", 1ms);
    t1.precede(t3);
    t2.precede(t3);
}

auto makeTimedFlow(tf::Taskflow& f1, const std::string& name, Clock::time_point& start_time, std::chrono::milliseconds delay) -> auto
{
    auto f1a = f1.emplace([&start_time, name](tf::Runtime& rt){
        start_time = Clock::now();
        });

    auto f1b = f1.emplace([name, delay](tf::Runtime& rt){
        auto start_time = Clock::now();
        std::this_thread::sleep_for(delay);
        auto end_time = Clock::now();
        log(rt.worker().id(), name, "work done", "elapsed", to_string(end_time-start_time));
        });

    auto f1c = f1.emplace([&start_time, name](tf::Runtime& rt){
        auto end_time = Clock::now();
        log(rt.worker().id(), name, "finished", "elapsed", to_string(end_time-start_time));
        });

    f1a.precede(f1b);
    f1b.precede(f1c);
    f1a.name(name + "_start_timed");
    f1b.name(name + "_work");
    f1c.name(name + "_finish_timed");
    return f1a;
}

auto makeTimedFlowWithModule(
    tf::Taskflow& f1, tf::Taskflow& sub, const std::string& name, Clock::time_point& start_time) -> auto
{
    auto f1a = f1.emplace([&start_time, name](tf::Runtime& rt){
        start_time = Clock::now();
        });

    makeInnerWorkFlow(sub, "mod");
    auto f1b = f1.composed_of(sub);

    auto f1c = f1.emplace([&start_time, name](tf::Runtime& rt){
        auto end_time = Clock::now();
        log(rt.worker().id(), name, "finished", "elapsed", to_string(end_time-start_time));
        });

    f1a.precede(f1b);
    f1b.precede(f1c);
    f1a.name(name + "_start_timed");
    f1b.name(name + "_work");
    f1c.name(name + "_finish_timed");
    return f1a;
}


auto s(std::chrono::milliseconds f5_duration) -> void 
{

    auto tftop = tf::Taskflow{};
    tftop.name("tftop");

    auto sub1 = tf::Taskflow{};
    sub1.name("sub1");
    auto start_time_f1 = Clock::time_point{};
    makeTimedFlowWithModule(tftop, sub1, "f1", start_time_f1);
    auto f2 = makeWorkFlow(tftop, "f2", 20ms);
    auto f3 = makeWorkFlow(tftop, "f3", 15ms);
    auto f4 = makeWorkFlow(tftop, "f4", 15ms);

    auto start_time_f5 = Clock::time_point{};
    auto f5 = makeTimedFlow(tftop, "f5", start_time_f5, f5_duration);

    f2.precede(f3);
    f2.precede(f4);
    f3.precede(f5);

    {
        auto d = std::ofstream("dump-time1.dot");
        tftop.dump(d);
    }

    auto executor = tf::Executor(3);
    // executor.make_observer<ExecutionObserver>();
    std::cerr << "Starting " + tftop.name() + " with f5_duration=" + to_string(f5_duration) + "\n";
    auto fut = executor.run(tftop,[]{std::cerr << "finish\n";});
    fut.get();
}
}  // namespace time1

auto main() -> int
{
    // s2::s2();
    // s6::s();
    time1::s(5ms);
    time1::s(500ms);
}