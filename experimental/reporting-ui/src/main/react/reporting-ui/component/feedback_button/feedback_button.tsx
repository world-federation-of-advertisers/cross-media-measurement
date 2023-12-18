/* Copyright 2023 The Cross-Media Measurement Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

import { Button } from 'react-bootstrap';
import './feedback_button.css';

const FeedbackButton = () => {
    return (
        <div id="fixed-bottom">
            <a href="https://docs.google.com/forms/d/e/1FAIpQLSfeLyJDETxQw92eyDiv2vA6XN9NpcIGHa0mbZweouqQ9mu8sg/viewform?usp=header_link" target="_blank">
                <Button variant="primary">
                    Feedback?
                </Button>
            </a>
        </div>
    )
}

export default FeedbackButton;