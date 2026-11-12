//go:build mqtt_fulltests

// Copyright 2026 The mqtt-nats-broker Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mqtt

// Recovering another claimant's abandoned proposal, end to end against the store.
//
// The decision tests establish WHICH continuation each settled verdict licenses. These
// establish that the continuations can actually RUN — the defect this closes twice over is an
// action returned by a decision function for which no transition kind exists, which is a
// route that was decided and not built.
