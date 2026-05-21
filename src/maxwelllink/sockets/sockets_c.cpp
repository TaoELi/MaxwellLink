// --------------------------------------------------------------------------------------//
// Copyright (c) 2026 MaxwellLink                                                        //
// This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   //
// If you use this code, always credit and cite arXiv:2512.06173.                        //
// See AGENTS.md and README.md for details.                                              //
// --------------------------------------------------------------------------------------//

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#ifndef _WIN32
#include <errno.h>
#include <poll.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

constexpr Py_ssize_t HEADER_LEN = 12;
constexpr Py_ssize_t FIELD_LEN = 24;
constexpr Py_ssize_t AGGSTEP_HEAD_LEN = 20;
constexpr Py_ssize_t AGGSTEP_RECORD_LEN = 8;
constexpr Py_ssize_t AGGRESULT_HEAD_LEN = 16;
constexpr Py_ssize_t AGGRESULT_RECORD_LEN = 32;
constexpr Py_ssize_t SOURCE_REPLY_FIXED_LEN = 124;

const char POSDATA_HDR[HEADER_LEN] = {'P', 'O', 'S', 'D', 'A', 'T', 'A', ' ', ' ', ' ', ' ', ' '};
const char GETFORCE_HDR[HEADER_LEN] = {'G', 'E', 'T', 'F', 'O', 'R', 'C', 'E', ' ', ' ', ' ', ' '};
const char FORCEREADY_HDR[HEADER_LEN] = {'F', 'O', 'R', 'C', 'E', 'R', 'E', 'A', 'D', 'Y', ' ', ' '};
const char AGGSTEP_HDR[HEADER_LEN] = {'A', 'G', 'G', 'S', 'T', 'E', 'P', ' ', ' ', ' ', ' ', ' '};
const char AGGRESULT_HDR[HEADER_LEN] = {'A', 'G', 'G', 'R', 'E', 'S', 'U', 'L', 'T', ' ', ' ', ' '};

using Clock = std::chrono::steady_clock;

bool is_little_endian() {
    const uint16_t x = 1;
    return *reinterpret_cast<const unsigned char *>(&x) == 1;
}

void write_i32(char *dst, int32_t value) {
    memcpy(dst, &value, sizeof(value));
}

int32_t read_i32(const char *src) {
    int32_t value;
    memcpy(&value, src, sizeof(value));
    return value;
}

void write_f64(char *dst, double value) {
    memcpy(dst, &value, sizeof(value));
}

double read_f64(const char *src) {
    double value;
    memcpy(&value, src, sizeof(value));
    return value;
}

void pad_header(char *dst, const char *msg, Py_ssize_t msg_len) {
    memset(dst, ' ', HEADER_LEN);
    memcpy(dst, msg, static_cast<size_t>(msg_len));
}

bool header_equals(const char *hdr, const char *expected) {
    return memcmp(hdr, expected, HEADER_LEN) == 0;
}

bool get_socket_fd_timeout(PyObject *sock, int *fd, double *timeout) {
    PyObject *fileno_obj = PyObject_CallMethod(sock, "fileno", nullptr);
    if (fileno_obj == nullptr) {
        return false;
    }
    long fd_long = PyLong_AsLong(fileno_obj);
    Py_DECREF(fileno_obj);
    if (fd_long < 0 || fd_long > std::numeric_limits<int>::max()) {
        if (!PyErr_Occurred()) {
            PyErr_SetString(PyExc_OSError, "Invalid socket file descriptor");
        }
        return false;
    }

    PyObject *timeout_obj = PyObject_CallMethod(sock, "gettimeout", nullptr);
    if (timeout_obj == nullptr) {
        return false;
    }
    if (timeout_obj == Py_None) {
        *timeout = -1.0;
    } else {
        *timeout = PyFloat_AsDouble(timeout_obj);
        if (PyErr_Occurred()) {
            Py_DECREF(timeout_obj);
            return false;
        }
    }
    Py_DECREF(timeout_obj);
    *fd = static_cast<int>(fd_long);
    return true;
}

int remaining_timeout_ms(double timeout, Clock::time_point deadline) {
    if (timeout < 0.0) {
        return -1;
    }
    auto now = Clock::now();
    if (now >= deadline) {
        return 0;
    }
    auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
    if (remaining <= 0) {
        return 1;
    }
    if (remaining > std::numeric_limits<int>::max()) {
        return std::numeric_limits<int>::max();
    }
    return static_cast<int>(remaining);
}

bool wait_for_fd(int fd, short events, double timeout, Clock::time_point deadline) {
    while (true) {
        int poll_timeout = remaining_timeout_ms(timeout, deadline);
        if (timeout >= 0.0 && poll_timeout == 0) {
            PyErr_SetString(PyExc_TimeoutError, "socket operation timed out");
            return false;
        }

        struct pollfd pfd;
        pfd.fd = fd;
        pfd.events = events;
        pfd.revents = 0;

        int rc;
        int saved_errno;
        Py_BEGIN_ALLOW_THREADS
        rc = poll(&pfd, 1, poll_timeout);
        saved_errno = errno;
        Py_END_ALLOW_THREADS

        if (rc > 0) {
            if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
                PyErr_SetString(PyExc_OSError, "socket poll reported an error");
                return false;
            }
            return true;
        }
        if (rc == 0) {
            PyErr_SetString(PyExc_TimeoutError, "socket operation timed out");
            return false;
        }
        if (saved_errno == EINTR) {
            continue;
        }
        errno = saved_errno;
        PyErr_SetFromErrno(PyExc_OSError);
        return false;
    }
}

bool send_all_fd(int fd, const char *data, Py_ssize_t len, double timeout) {
    auto deadline = Clock::now() + std::chrono::microseconds(
        timeout < 0.0 ? 0 : static_cast<long long>(timeout * 1000000.0));
    Py_ssize_t sent = 0;
    while (sent < len) {
        if (!wait_for_fd(fd, POLLOUT, timeout, deadline)) {
            return false;
        }

        ssize_t n;
        int saved_errno;
        int flags = 0;
#ifdef MSG_NOSIGNAL
        flags |= MSG_NOSIGNAL;
#endif
        Py_BEGIN_ALLOW_THREADS
        n = send(fd, data + sent, static_cast<size_t>(len - sent), flags);
        saved_errno = errno;
        Py_END_ALLOW_THREADS

        if (n > 0) {
            sent += static_cast<Py_ssize_t>(n);
            continue;
        }
        if (n == 0) {
            PyErr_SetString(PyExc_OSError, "socket send returned zero bytes");
            return false;
        }
        if (saved_errno == EINTR || saved_errno == EAGAIN || saved_errno == EWOULDBLOCK) {
            continue;
        }
        errno = saved_errno;
        PyErr_SetFromErrno(PyExc_OSError);
        return false;
    }
    return true;
}

bool recv_exact_fd(int fd, char *data, Py_ssize_t len, double timeout) {
    auto deadline = Clock::now() + std::chrono::microseconds(
        timeout < 0.0 ? 0 : static_cast<long long>(timeout * 1000000.0));
    Py_ssize_t got = 0;
    while (got < len) {
        if (!wait_for_fd(fd, POLLIN, timeout, deadline)) {
            return false;
        }

        ssize_t n;
        int saved_errno;
        Py_BEGIN_ALLOW_THREADS
        n = recv(fd, data + got, static_cast<size_t>(len - got), 0);
        saved_errno = errno;
        Py_END_ALLOW_THREADS

        if (n > 0) {
            got += static_cast<Py_ssize_t>(n);
            continue;
        }
        if (n == 0) {
            PyErr_SetString(PyExc_OSError, "Peer closed");
            return false;
        }
        if (saved_errno == EINTR || saved_errno == EAGAIN || saved_errno == EWOULDBLOCK) {
            continue;
        }
        errno = saved_errno;
        PyErr_SetFromErrno(PyExc_OSError);
        return false;
    }
    return true;
}

bool native_send_all(PyObject *sock, const char *data, Py_ssize_t len) {
    int fd;
    double timeout;
    if (!get_socket_fd_timeout(sock, &fd, &timeout)) {
        return false;
    }
    return send_all_fd(fd, data, len, timeout);
}

bool native_recv_exact(PyObject *sock, char *data, Py_ssize_t len) {
    int fd;
    double timeout;
    if (!get_socket_fd_timeout(sock, &fd, &timeout)) {
        return false;
    }
    return recv_exact_fd(fd, data, len, timeout);
}

bool extract_vec3(PyObject *obj, double out[3]) {
    Py_buffer view;
    if (PyObject_GetBuffer(obj, &view, PyBUF_CONTIG_RO) == 0) {
        if (view.len >= static_cast<Py_ssize_t>(3 * sizeof(double))) {
            memcpy(out, view.buf, 3 * sizeof(double));
            PyBuffer_Release(&view);
            return true;
        }
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "Expected at least three float64 values");
        return false;
    }
    PyErr_Clear();

    PyObject *seq = PySequence_Fast(obj, "Expected a length-3 vector");
    if (seq == nullptr) {
        return false;
    }
    if (PySequence_Fast_GET_SIZE(seq) < 3) {
        Py_DECREF(seq);
        PyErr_SetString(PyExc_ValueError, "Expected a length-3 vector");
        return false;
    }
    for (Py_ssize_t i = 0; i < 3; ++i) {
        out[i] = PyFloat_AsDouble(PySequence_Fast_GET_ITEM(seq, i));
        if (PyErr_Occurred()) {
            Py_DECREF(seq);
            return false;
        }
    }
    Py_DECREF(seq);
    return true;
}

PyObject *get_mapping_item_string_optional(PyObject *mapping, const char *key) {
    PyObject *value = PyMapping_GetItemString(mapping, key);
    if (value == nullptr) {
        PyErr_Clear();
    }
    return value;
}

bool extract_bytes(PyObject *obj, std::string *out) {
    if (obj == nullptr || obj == Py_None) {
        out->clear();
        return true;
    }

    PyObject *owned = nullptr;
    PyObject *source = obj;
    if (PyUnicode_Check(obj)) {
        owned = PyUnicode_AsUTF8String(obj);
        if (owned == nullptr) {
            return false;
        }
        source = owned;
    }

    Py_buffer view;
    if (PyObject_GetBuffer(source, &view, PyBUF_CONTIG_RO) != 0) {
        Py_XDECREF(owned);
        return false;
    }
    out->assign(static_cast<const char *>(view.buf), static_cast<size_t>(view.len));
    PyBuffer_Release(&view);
    Py_XDECREF(owned);
    return true;
}

PyObject *py_send_all(PyObject *, PyObject *args) {
    PyObject *sock;
    PyObject *payload;
    if (!PyArg_ParseTuple(args, "OO:send_all", &sock, &payload)) {
        return nullptr;
    }

    Py_buffer view;
    if (PyObject_GetBuffer(payload, &view, PyBUF_CONTIG_RO) != 0) {
        return nullptr;
    }
    bool ok = native_send_all(sock, static_cast<const char *>(view.buf), view.len);
    PyBuffer_Release(&view);
    if (!ok) {
        return nullptr;
    }
    Py_RETURN_NONE;
}

PyObject *py_recv_exact(PyObject *, PyObject *args) {
    PyObject *sock;
    Py_ssize_t nbytes;
    if (!PyArg_ParseTuple(args, "On:recv_exact", &sock, &nbytes)) {
        return nullptr;
    }
    if (nbytes < 0) {
        PyErr_SetString(PyExc_ValueError, "nbytes must be non-negative");
        return nullptr;
    }

    PyObject *out = PyBytes_FromStringAndSize(nullptr, nbytes);
    if (out == nullptr) {
        return nullptr;
    }
    if (!native_recv_exact(sock, PyBytes_AS_STRING(out), nbytes)) {
        Py_DECREF(out);
        return nullptr;
    }
    return out;
}

PyObject *py_send_msg(PyObject *, PyObject *args) {
    PyObject *sock;
    const char *msg;
    Py_ssize_t msg_len;
    if (!PyArg_ParseTuple(args, "Oy#:send_msg", &sock, &msg, &msg_len)) {
        return nullptr;
    }
    if (msg_len > HEADER_LEN) {
        PyErr_SetString(PyExc_ValueError, "Header too long");
        return nullptr;
    }
    char hdr[HEADER_LEN];
    pad_header(hdr, msg, msg_len);
    if (!native_send_all(sock, hdr, HEADER_LEN)) {
        return nullptr;
    }
    Py_RETURN_NONE;
}

PyObject *py_recv_msg(PyObject *, PyObject *args) {
    PyObject *sock;
    if (!PyArg_ParseTuple(args, "O:recv_msg", &sock)) {
        return nullptr;
    }
    char hdr[HEADER_LEN];
    if (!native_recv_exact(sock, hdr, HEADER_LEN)) {
        return nullptr;
    }
    Py_ssize_t end = HEADER_LEN;
    while (end > 0 && hdr[end - 1] == ' ') {
        --end;
    }
    return PyBytes_FromStringAndSize(hdr, end);
}

PyObject *py_send_int(PyObject *, PyObject *args) {
    PyObject *sock;
    long value;
    if (!PyArg_ParseTuple(args, "Ol:send_int", &sock, &value)) {
        return nullptr;
    }
    if (value < std::numeric_limits<int32_t>::min() || value > std::numeric_limits<int32_t>::max()) {
        PyErr_SetString(PyExc_OverflowError, "Integer does not fit in int32");
        return nullptr;
    }
    char buf[4];
    write_i32(buf, static_cast<int32_t>(value));
    if (!native_send_all(sock, buf, 4)) {
        return nullptr;
    }
    Py_RETURN_NONE;
}

PyObject *py_recv_int(PyObject *, PyObject *args) {
    PyObject *sock;
    if (!PyArg_ParseTuple(args, "O:recv_int", &sock)) {
        return nullptr;
    }
    char buf[4];
    if (!native_recv_exact(sock, buf, 4)) {
        return nullptr;
    }
    return PyLong_FromLong(read_i32(buf));
}

PyObject *py_send_bytes(PyObject *, PyObject *args) {
    PyObject *sock;
    PyObject *payload;
    if (!PyArg_ParseTuple(args, "OO:send_bytes", &sock, &payload)) {
        return nullptr;
    }
    Py_buffer view;
    if (PyObject_GetBuffer(payload, &view, PyBUF_CONTIG_RO) != 0) {
        return nullptr;
    }
    if (view.len > std::numeric_limits<int32_t>::max()) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_OverflowError, "Byte payload too large");
        return nullptr;
    }
    char len_buf[4];
    write_i32(len_buf, static_cast<int32_t>(view.len));
    bool ok = native_send_all(sock, len_buf, 4);
    if (ok && view.len > 0) {
        ok = native_send_all(sock, static_cast<const char *>(view.buf), view.len);
    }
    PyBuffer_Release(&view);
    if (!ok) {
        return nullptr;
    }
    Py_RETURN_NONE;
}

PyObject *py_recv_bytes(PyObject *, PyObject *args) {
    PyObject *sock;
    if (!PyArg_ParseTuple(args, "O:recv_bytes", &sock)) {
        return nullptr;
    }
    char len_buf[4];
    if (!native_recv_exact(sock, len_buf, 4)) {
        return nullptr;
    }
    int32_t nbytes = read_i32(len_buf);
    if (nbytes < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Negative byte payload length");
        return nullptr;
    }
    PyObject *out = PyBytes_FromStringAndSize(nullptr, nbytes);
    if (out == nullptr) {
        return nullptr;
    }
    if (nbytes > 0 && !native_recv_exact(sock, PyBytes_AS_STRING(out), nbytes)) {
        Py_DECREF(out);
        return nullptr;
    }
    return out;
}

PyObject *py_recv_source_ready(PyObject *, PyObject *args) {
    PyObject *sock;
    if (!PyArg_ParseTuple(args, "O:recv_source_ready", &sock)) {
        return nullptr;
    }
    char fixed[SOURCE_REPLY_FIXED_LEN];
    if (!native_recv_exact(sock, fixed, SOURCE_REPLY_FIXED_LEN)) {
        return nullptr;
    }
    if (!header_equals(fixed, FORCEREADY_HDR)) {
        PyErr_SetString(PyExc_RuntimeError, "Expected FORCEREADY reply");
        return nullptr;
    }
    int32_t nat = read_i32(fixed + 20);
    if (nat != 1) {
        PyErr_SetString(PyExc_RuntimeError, "EM fast-path expected nat=1");
        return nullptr;
    }
    int32_t extra_len = read_i32(fixed + 120);
    if (extra_len < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Negative extra payload length");
        return nullptr;
    }

    PyObject *extra = PyBytes_FromStringAndSize(nullptr, extra_len);
    if (extra == nullptr) {
        return nullptr;
    }
    if (extra_len > 0 && !native_recv_exact(sock, PyBytes_AS_STRING(extra), extra_len)) {
        Py_DECREF(extra);
        return nullptr;
    }

    double fx = read_f64(fixed + 24);
    double fy = read_f64(fixed + 32);
    double fz = read_f64(fixed + 40);
    return Py_BuildValue("(dddN)", fx, fy, fz, extra);
}

PyObject *py_send_field_request(PyObject *, PyObject *args) {
    PyObject *sock;
    double fx, fy, fz;
    if (!PyArg_ParseTuple(args, "Oddd:send_field_request", &sock, &fx, &fy, &fz)) {
        return nullptr;
    }

    char buf[196];
    memcpy(buf, POSDATA_HDR, HEADER_LEN);
    Py_ssize_t offset = HEADER_LEN;
    for (int matrix = 0; matrix < 2; ++matrix) {
        for (int i = 0; i < 9; ++i) {
            write_f64(buf + offset, (i == 0 || i == 4 || i == 8) ? 1.0 : 0.0);
            offset += 8;
        }
    }
    write_i32(buf + offset, 1);
    offset += 4;
    write_f64(buf + offset, fx);
    write_f64(buf + offset + 8, fy);
    write_f64(buf + offset + 16, fz);
    offset += 24;
    memcpy(buf + offset, GETFORCE_HDR, HEADER_LEN);

    if (!native_send_all(sock, buf, sizeof(buf))) {
        return nullptr;
    }
    Py_RETURN_NONE;
}

struct StepMember {
    int32_t molecule_id;
    int32_t field_idx;
};

PyObject *py_encode_step_frame(PyObject *, PyObject *args) {
    PyObject *requests;
    if (!PyArg_ParseTuple(args, "O:encode_step_frame", &requests)) {
        return nullptr;
    }
    PyObject *items = PyMapping_Items(requests);
    if (items == nullptr) {
        return nullptr;
    }
    Py_ssize_t nreq = PyList_GET_SIZE(items);
    if (nreq > std::numeric_limits<int32_t>::max()) {
        Py_DECREF(items);
        PyErr_SetString(PyExc_OverflowError, "Too many aggregate requests");
        return nullptr;
    }

    std::vector<std::array<double, 3>> fields;
    std::vector<StepMember> members;
    std::unordered_map<std::string, int32_t> field_to_idx;
    fields.reserve(static_cast<size_t>(nreq));
    members.reserve(static_cast<size_t>(nreq));

    for (Py_ssize_t i = 0; i < nreq; ++i) {
        PyObject *item = PyList_GET_ITEM(items, i);
        PyObject *mid_obj = PyTuple_GET_ITEM(item, 0);
        PyObject *payload = PyTuple_GET_ITEM(item, 1);
        long mid_long = PyLong_AsLong(mid_obj);
        if (PyErr_Occurred() || mid_long < std::numeric_limits<int32_t>::min() ||
            mid_long > std::numeric_limits<int32_t>::max()) {
            Py_DECREF(items);
            PyErr_SetString(PyExc_ValueError, "Molecule id does not fit in int32");
            return nullptr;
        }
        PyObject *efield_obj = PyMapping_GetItemString(payload, "efield_au");
        if (efield_obj == nullptr) {
            Py_DECREF(items);
            return nullptr;
        }
        double vec[3];
        bool ok = extract_vec3(efield_obj, vec);
        Py_DECREF(efield_obj);
        if (!ok) {
            Py_DECREF(items);
            return nullptr;
        }
        std::string key(reinterpret_cast<char *>(vec), 3 * sizeof(double));
        auto found = field_to_idx.find(key);
        int32_t field_idx;
        if (found == field_to_idx.end()) {
            field_idx = static_cast<int32_t>(fields.size());
            fields.push_back({vec[0], vec[1], vec[2]});
            field_to_idx.emplace(std::move(key), field_idx);
        } else {
            field_idx = found->second;
        }
        members.push_back({static_cast<int32_t>(mid_long), field_idx});
    }
    Py_DECREF(items);

    Py_ssize_t nuniq = static_cast<Py_ssize_t>(fields.size());
    Py_ssize_t frame_len = AGGSTEP_HEAD_LEN + FIELD_LEN * nuniq + AGGSTEP_RECORD_LEN * nreq;
    PyObject *frame = PyBytes_FromStringAndSize(nullptr, frame_len);
    if (frame == nullptr) {
        return nullptr;
    }
    char *buf = PyBytes_AS_STRING(frame);
    memcpy(buf, AGGSTEP_HDR, HEADER_LEN);
    write_i32(buf + HEADER_LEN, static_cast<int32_t>(nreq));
    write_i32(buf + HEADER_LEN + 4, static_cast<int32_t>(nuniq));

    Py_ssize_t offset = AGGSTEP_HEAD_LEN;
    for (const auto &field : fields) {
        write_f64(buf + offset, field[0]);
        write_f64(buf + offset + 8, field[1]);
        write_f64(buf + offset + 16, field[2]);
        offset += FIELD_LEN;
    }
    for (const auto &member : members) {
        write_i32(buf + offset, member.molecule_id);
        write_i32(buf + offset + 4, member.field_idx);
        offset += AGGSTEP_RECORD_LEN;
    }
    return frame;
}

PyObject *py_decode_step_frame(PyObject *, PyObject *args, PyObject *kwargs) {
    PyObject *sock;
    int header_already_read = 0;
    static const char *kwlist[] = {"sock", "header_already_read", nullptr};
    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "O|p:decode_step_frame", const_cast<char **>(kwlist),
            &sock, &header_already_read)) {
        return nullptr;
    }

    char head[AGGSTEP_HEAD_LEN];
    if (header_already_read) {
        memcpy(head, AGGSTEP_HDR, HEADER_LEN);
        if (!native_recv_exact(sock, head + HEADER_LEN, AGGSTEP_HEAD_LEN - HEADER_LEN)) {
            return nullptr;
        }
    } else if (!native_recv_exact(sock, head, AGGSTEP_HEAD_LEN)) {
        return nullptr;
    }
    if (!header_equals(head, AGGSTEP_HDR)) {
        PyErr_SetString(PyExc_RuntimeError, "Expected AGGSTEP frame");
        return nullptr;
    }
    int32_t nreq = read_i32(head + HEADER_LEN);
    int32_t nuniq = read_i32(head + HEADER_LEN + 4);
    if (nreq < 0 || nuniq < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Negative aggregate frame size");
        return nullptr;
    }

    Py_ssize_t body_len = FIELD_LEN * static_cast<Py_ssize_t>(nuniq) +
                          AGGSTEP_RECORD_LEN * static_cast<Py_ssize_t>(nreq);
    std::vector<char> body(static_cast<size_t>(body_len));
    if (body_len > 0 && !native_recv_exact(sock, body.data(), body_len)) {
        return nullptr;
    }

    std::vector<std::array<double, 3>> fields(static_cast<size_t>(nuniq));
    Py_ssize_t offset = 0;
    for (int32_t i = 0; i < nuniq; ++i) {
        fields[static_cast<size_t>(i)] = {
            read_f64(body.data() + offset),
            read_f64(body.data() + offset + 8),
            read_f64(body.data() + offset + 16),
        };
        offset += FIELD_LEN;
    }

    PyObject *rows = PyList_New(nreq);
    if (rows == nullptr) {
        return nullptr;
    }
    for (int32_t i = 0; i < nreq; ++i) {
        int32_t mid = read_i32(body.data() + offset);
        int32_t field_idx = read_i32(body.data() + offset + 4);
        offset += AGGSTEP_RECORD_LEN;
        if (field_idx < 0 || field_idx >= nuniq) {
            Py_DECREF(rows);
            PyErr_SetString(PyExc_RuntimeError, "Invalid aggregate field index");
            return nullptr;
        }
        const auto &field = fields[static_cast<size_t>(field_idx)];
        PyObject *row = Py_BuildValue("(i(ddd))", mid, field[0], field[1], field[2]);
        if (row == nullptr) {
            Py_DECREF(rows);
            return nullptr;
        }
        PyList_SET_ITEM(rows, i, row);
    }
    return rows;
}

struct ResultItem {
    int32_t molecule_id;
    std::array<double, 3> amp;
    std::string extra;
};

PyObject *py_encode_result_frame(PyObject *, PyObject *args) {
    PyObject *responses;
    if (!PyArg_ParseTuple(args, "O:encode_result_frame", &responses)) {
        return nullptr;
    }
    PyObject *items = PyMapping_Items(responses);
    if (items == nullptr) {
        return nullptr;
    }
    Py_ssize_t nresp = PyList_GET_SIZE(items);
    if (nresp > std::numeric_limits<int32_t>::max()) {
        Py_DECREF(items);
        PyErr_SetString(PyExc_OverflowError, "Too many aggregate responses");
        return nullptr;
    }

    std::vector<ResultItem> packed;
    packed.reserve(static_cast<size_t>(nresp));
    Py_ssize_t total_extra = 0;
    for (Py_ssize_t i = 0; i < nresp; ++i) {
        PyObject *item = PyList_GET_ITEM(items, i);
        PyObject *mid_obj = PyTuple_GET_ITEM(item, 0);
        PyObject *payload = PyTuple_GET_ITEM(item, 1);
        long mid_long = PyLong_AsLong(mid_obj);
        if (PyErr_Occurred() || mid_long < std::numeric_limits<int32_t>::min() ||
            mid_long > std::numeric_limits<int32_t>::max()) {
            Py_DECREF(items);
            PyErr_SetString(PyExc_ValueError, "Molecule id does not fit in int32");
            return nullptr;
        }
        PyObject *amp_obj = PyMapping_GetItemString(payload, "amp");
        if (amp_obj == nullptr) {
            Py_DECREF(items);
            return nullptr;
        }
        double amp[3];
        bool ok = extract_vec3(amp_obj, amp);
        Py_DECREF(amp_obj);
        if (!ok) {
            Py_DECREF(items);
            return nullptr;
        }
        PyObject *extra_obj = get_mapping_item_string_optional(payload, "extra");
        std::string extra;
        ok = extract_bytes(extra_obj, &extra);
        Py_XDECREF(extra_obj);
        if (!ok) {
            Py_DECREF(items);
            return nullptr;
        }
        if (extra.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
            Py_DECREF(items);
            PyErr_SetString(PyExc_OverflowError, "Aggregate extra payload too large");
            return nullptr;
        }
        total_extra += static_cast<Py_ssize_t>(extra.size());
        packed.push_back(
            {static_cast<int32_t>(mid_long), {amp[0], amp[1], amp[2]}, std::move(extra)});
    }
    Py_DECREF(items);

    Py_ssize_t fixed_len = AGGRESULT_HEAD_LEN + AGGRESULT_RECORD_LEN * nresp;
    Py_ssize_t frame_len = fixed_len + total_extra;
    PyObject *frame = PyBytes_FromStringAndSize(nullptr, frame_len);
    if (frame == nullptr) {
        return nullptr;
    }
    char *buf = PyBytes_AS_STRING(frame);
    memcpy(buf, AGGRESULT_HDR, HEADER_LEN);
    write_i32(buf + HEADER_LEN, static_cast<int32_t>(nresp));

    Py_ssize_t offset = AGGRESULT_HEAD_LEN;
    Py_ssize_t extra_offset = fixed_len;
    for (const auto &item : packed) {
        write_i32(buf + offset, item.molecule_id);
        write_f64(buf + offset + 4, item.amp[0]);
        write_f64(buf + offset + 12, item.amp[1]);
        write_f64(buf + offset + 20, item.amp[2]);
        write_i32(buf + offset + 28, static_cast<int32_t>(item.extra.size()));
        offset += AGGRESULT_RECORD_LEN;
        if (!item.extra.empty()) {
            memcpy(buf + extra_offset, item.extra.data(), item.extra.size());
            extra_offset += static_cast<Py_ssize_t>(item.extra.size());
        }
    }
    return frame;
}

PyObject *py_decode_result_frame(PyObject *, PyObject *args) {
    PyObject *sock;
    if (!PyArg_ParseTuple(args, "O:decode_result_frame", &sock)) {
        return nullptr;
    }
    char head[AGGRESULT_HEAD_LEN];
    if (!native_recv_exact(sock, head, AGGRESULT_HEAD_LEN)) {
        return nullptr;
    }
    if (!header_equals(head, AGGRESULT_HDR)) {
        PyErr_SetString(PyExc_RuntimeError, "Expected AGGRESULT frame");
        return nullptr;
    }
    int32_t nresp = read_i32(head + HEADER_LEN);
    if (nresp < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Negative aggregate response count");
        return nullptr;
    }
    Py_ssize_t fixed_len = AGGRESULT_RECORD_LEN * static_cast<Py_ssize_t>(nresp);
    std::vector<char> fixed(static_cast<size_t>(fixed_len));
    if (fixed_len > 0 && !native_recv_exact(sock, fixed.data(), fixed_len)) {
        return nullptr;
    }

    struct MetaItem {
        int32_t molecule_id;
        std::array<double, 3> amp;
        int32_t extra_len;
    };
    std::vector<MetaItem> meta;
    meta.reserve(static_cast<size_t>(nresp));
    Py_ssize_t total_extra = 0;
    Py_ssize_t offset = 0;
    for (int32_t i = 0; i < nresp; ++i) {
        int32_t mid = read_i32(fixed.data() + offset);
        double fx = read_f64(fixed.data() + offset + 4);
        double fy = read_f64(fixed.data() + offset + 12);
        double fz = read_f64(fixed.data() + offset + 20);
        int32_t extra_len = read_i32(fixed.data() + offset + 28);
        offset += AGGRESULT_RECORD_LEN;
        if (extra_len < 0) {
            PyErr_SetString(PyExc_RuntimeError, "Negative aggregate extra payload length");
            return nullptr;
        }
        total_extra += extra_len;
        meta.push_back({mid, {fx, fy, fz}, extra_len});
    }

    std::vector<char> extras(static_cast<size_t>(total_extra));
    if (total_extra > 0 && !native_recv_exact(sock, extras.data(), total_extra)) {
        return nullptr;
    }

    PyObject *rows = PyList_New(nresp);
    if (rows == nullptr) {
        return nullptr;
    }
    Py_ssize_t extra_offset = 0;
    for (int32_t i = 0; i < nresp; ++i) {
        const auto &item = meta[static_cast<size_t>(i)];
        PyObject *extra = PyBytes_FromStringAndSize(
            item.extra_len ? extras.data() + extra_offset : "", item.extra_len);
        if (extra == nullptr) {
            Py_DECREF(rows);
            return nullptr;
        }
        extra_offset += item.extra_len;
        PyObject *row = Py_BuildValue(
            "(i(ddd)N)", item.molecule_id, item.amp[0], item.amp[1], item.amp[2], extra);
        if (row == nullptr) {
            Py_DECREF(rows);
            return nullptr;
        }
        PyList_SET_ITEM(rows, i, row);
    }
    return rows;
}

PyMethodDef methods[] = {
    {"send_all", py_send_all, METH_VARARGS, "Send a complete bytes-like payload on a socket fd."},
    {"recv_exact", py_recv_exact, METH_VARARGS, "Receive an exact number of bytes from a socket fd."},
    {"send_msg", py_send_msg, METH_VARARGS, "Send a MaxwellLink 12-byte padded header."},
    {"recv_msg", py_recv_msg, METH_VARARGS, "Receive a MaxwellLink 12-byte padded header."},
    {"send_int", py_send_int, METH_VARARGS, "Send a little-endian int32."},
    {"recv_int", py_recv_int, METH_VARARGS, "Receive a little-endian int32."},
    {"send_bytes", py_send_bytes, METH_VARARGS, "Send an int32-length-prefixed byte string."},
    {"recv_bytes", py_recv_bytes, METH_VARARGS, "Receive an int32-length-prefixed byte string."},
    {"recv_source_ready", py_recv_source_ready, METH_VARARGS, "Receive a fixed SOURCEREADY reply."},
    {"send_field_request", py_send_field_request, METH_VARARGS, "Send one FIELDDATA+GETSOURCE request."},
    {"encode_step_frame", py_encode_step_frame, METH_VARARGS, "Encode an aggregate STEP frame."},
    {"decode_step_frame", reinterpret_cast<PyCFunction>(py_decode_step_frame), METH_VARARGS | METH_KEYWORDS, "Decode an aggregate STEP frame."},
    {"encode_result_frame", py_encode_result_frame, METH_VARARGS, "Encode an aggregate RESULT frame."},
    {"decode_result_frame", py_decode_result_frame, METH_VARARGS, "Decode an aggregate RESULT frame."},
    {nullptr, nullptr, 0, nullptr},
};

PyModuleDef module = {
    PyModuleDef_HEAD_INIT,
    "maxwelllink.sockets._csockets",
    "Native helpers for MaxwellLink socket protocol hot paths.",
    -1,
    methods,
};

}  // namespace

PyMODINIT_FUNC PyInit__csockets(void) {
    if (!is_little_endian()) {
        PyErr_SetString(PyExc_RuntimeError, "MaxwellLink native socket helpers require a little-endian host");
        return nullptr;
    }
    return PyModule_Create(&module);
}

#else

PyMODINIT_FUNC PyInit__csockets(void) {
    PyErr_SetString(PyExc_ImportError, "MaxwellLink native socket helpers are not available on Windows");
    return nullptr;
}

#endif
