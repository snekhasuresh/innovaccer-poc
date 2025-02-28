<?php
    function enqueue_crashTest_info_tab_css()
    {
        global $post;
    //     if (isset($post->post_content) && has_shortcode($post->post_content, 'brands_list')) {
            wp_enqueue_style(
                'crash-rating-style',
                get_stylesheet_directory_uri() . '/widget-shortcodes/crash-test/css/test-info-tab.css',
                array(),
                '1.0',
                'all'
            );
    //     }
    }
    add_action('wp_enqueue_scripts', 'enqueue_crashTest_info_tab_css');
function crash_test_table_widget() {
    enqueue_crashTest_info_tab_css();
    ob_start();
    ?>

   <div class="crash-test-tabs-container">
      <button class="crash-test-arrow crash-test-left" onclick="scrollTabs(-100)"></button>
      <nav class="crash-test-tabs" onscroll="toggleArrows()">
        <div class="crash-test-tab crash-test-active" data-index="0">สเปค</div>
        <div class="crash-test-tab" data-index="1">การปกป้องผู้โดยสารที่เป็นเด็ก (COP)</div>
        <div class="crash-test-tab" data-index="2">การปกป้องผู้โดยสารที่เป็นผู้ใหญ่ (AOP)</div>
        <div class="crash-test-tab" data-index="3">เทคโนโลยีเพื่อความปลอดภัย (STAs)</div>
        <div class="crash-test-tab" data-index="4">Variant Availability</div>
        <div class="crash-test-tab" data-index="5">Video</div>
      </nav>
      <button class="crash-test-arrow crash-test-right" onclick="scrollTabs(100)"></button>
  </div>


  <section class="crash-test-tab-content crash-test-active">
      <table class="crash-test-spec-table">
        <tbody id="crash-test-spec-table-body"></tbody>
      </table>
  </section>


  <section class="crash-test-tab-content">
      <table class="crash-test-aop-table">
        <thead>
          <tr>
            <th>ITEM</th>
            <th>SCORE</th>
          </tr>
        </thead>
        <tbody id="crash-test-aop-table-body"></tbody>
      </table>


      <div class="crash-test-footnote">*Score calculated based on Fitment Rating System</div>


      <table class="crash-test-passive-safety-table">
        <thead>
          <tr>
            <th>PASSIVE SAFETY</th>
            <th>ASEAN</th>
          </tr>
        </thead>
        <tbody id="crash-test-passive-safety-table"></tbody>
      </table>
  </section>
  <section class="crash-test-tab-content">
      <img
        src="https://carnetwork.s3.ap-southeast-1.amazonaws.com/file/aac9a154807045aabaffb04f20c12eb6.png"
        alt="COP Test Image"
        class="crash-test-cop-image"
      />
  </section>


  <section class="crash-test-tab-content">
      <table class="crash-test-safety-table">
        <thead>
          <tr>
            <th>ACTIVE SAFETY</th>
            <th>ASEAN</th>
          </tr>
        </thead>
        <tbody id="crash-test-stas-table"></tbody>
      </table>
  </section>


  <!-- Variant Availability Tab -->
  <section class="crash-test-tab-content">
      <table class="crash-test-variant-table">
        <thead>
          <tr>
            <th>Variant</th>
            <th>Country</th>
            <th>Availability</th>
          </tr>
        </thead>
        <tbody id="crash-test-variant-table-body"></tbody>
      </table>
  </section>
  <section class="crash-test-tab-content">
      <div class="crash-test-video-box">
        <iframe
          frameborder="0"
          allowfullscreen
          allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
          referrerpolicy="strict-origin-when-cross-origin"
          title="ASEAN NCAP - Honda Civic (2021)"
          width="800"
          height="450"
          src="https://www.youtube.com/embed/8BVJZoAGBYU?enablejsapi=1&origin=https%3A%2F%2Fwww.autofun.co.th&widgetid=1"
          id="widget2"
        >
        </iframe>
      </div>
  </section>
  <script>
      function scrollTabs(amount) {
        const tabs = document.querySelector(".crash-test-tabs");
        tabs.scrollBy({ left: amount, behavior: "smooth" });
        setTimeout(toggleArrows, 300);
      }


      function toggleArrows() {
        const tabs = document.querySelector(".crash-test-tabs");
        const leftArrow = document.querySelector(".crash-test-arrow.crash-test-left");
        const rightArrow = document.querySelector(".crash-test-arrow.crash-test-right");


        if (tabs.scrollWidth > tabs.clientWidth) {
          rightArrow.classList.add("crash-test-visible");
        } else {
          rightArrow.classList.remove("crash-test-visible");
          leftArrow.classList.remove("crash-test-visible");
        }


        if (tabs.scrollLeft > 0) {
          leftArrow.classList.add("crash-test-visible");
        } else {
          leftArrow.classList.remove("crash-test-visible");
        }


        if (tabs.scrollLeft + tabs.clientWidth >= tabs.scrollWidth) {
          rightArrow.classList.remove("crash-test-visible");
        }
      }


      document.addEventListener("DOMContentLoaded", toggleArrows);


      document.querySelectorAll(".crash-test-tab").forEach((tab) => {
        tab.addEventListener("click", () => {
          document.querySelectorAll(".crash-test-tab").forEach((t) => t.classList.remove("crash-test-active"));
          document.querySelectorAll(".crash-test-tab-content").forEach((content) => content.classList.remove("crash-test-active"));
          tab.classList.add("crash-test-active");
          document.querySelectorAll(".crash-test-tab-content")[tab.dataset.index].classList.add("crash-test-active");
        });
      });


      // Data for tables
      const specData = [
        ["รุ่น", "EL+"],
        ["ปีที่ผลิต", "2021"],
        ["ปีที่เปิดตัว", "2021"],
        ["ประเภทรถยนต์", "SEDAN"],
        ["ความจุเครื่องยนต์", "1.5 L"],
        ["น้ำหนักรถยนต์พร้อมที่จะวิ่งได้", "1319 KG"],
        ["ห้องปฏิบัติการทดสอบ", "JARI"],
      ];


      const aopData = [
        ["OFFSET FRONTAL TEST", "14.54"],
        ["SIDE IMPACT TEST", "8.00"],
        ["HEAD PROTECTION TECHNOLOGY EVALUATION*", "6.74"],
      ];


      const passiveSafetyData = [
        ["DRIVER AIRBAG", "มาตรฐานทุกรุ่นย่อย"],
        ["FRONT PASSENGER AIRBAG", "มาตรฐานทุกรุ่นย่อย"],
        ["CURTAIN AIRBAG", "มีให้เลือกทั้งแบบมาตรฐานและอุปกรณ์เสริม"],
        ["SIDE AIRBAG", "มาตรฐานทุกรุ่นย่อย"],
        ["KNEE AIRBAG", "ไม่สามารถใช้ได้"],
        ["SEATBELT RETRACTOR & LOAD LIMITER - DRIVER", "มาตรฐานทุกรุ่นย่อย"],
      ];


      const stasData = [
        {
          category: "EFFECTIVE BRAKING & AVOIDANCE",
          data: [
            ["ABS", "มาตรฐานทุกรุ่นย่อย"],
            ["ESC", "มาตรฐานทุกรุ่นย่อย"],
          ],
        },
        {
          category: "SEATBELT REMINDERS",
          data: [
            ["SBR FOR DRIVER", "มาตรฐานทุกรุ่นย่อย"],
            ["SBR FOR FRONT PASSENGER", "มาตรฐานทุกรุ่นย่อย"],
            ["SBR FOR REAR PASSENGERS", "AVAILABLE AS STANDARD OF OPTIONAL"],
            ["SBR FOR REAR SEAT OCCUPANT DETECTION", "ไม่สามารถใช้ได้"],
          ],
        },
        {
          category: "AUTONOMOUS EMERGENCY BRAKING",
          data: [
            ["AEB CITY", "มาตรฐานทุกรุ่นย่อย"],
            ["AEB INTER-URBAN", "มาตรฐานทุกรุ่นย่อย"],
          ],
        },
      ];


      const variantAvailabilityData = [
        ["ที่มาของรุ่นที่เข้าการทดสอบ", "THAILAND", "FOR THAILAND"],
        ["รุ่นอื่น ๆ ในเอเชีย (ทุกรุ่น)", "THAILAND", "FOR THAILAND, SINGAPORE, INDONESIA"],
      ];


      // Populate functions
      function populateTable(tableId, data) {
        const tableBody = document.getElementById(tableId);
        tableBody.innerHTML = ""; // Clear any existing content
        data.forEach((row) => {
          const tr = document.createElement("tr");
          row.forEach((cell) => {
            const td = document.createElement("td");
            td.textContent = cell;
            tr.appendChild(td);
          });
          tableBody.appendChild(tr);
        });
      }


      function populateStasTable() {
        const tableBody = document.getElementById("crash-test-stas-table");
        tableBody.innerHTML = ""; // Clear any existing content


        stasData.forEach((section) => {
          const categoryRow = document.createElement("tr");
          const categoryCell = document.createElement("td");
          categoryCell.colSpan = 2;
          categoryCell.classList.add("crash-test-section-header");
          categoryCell.textContent = section.category;
          categoryRow.appendChild(categoryCell);
          tableBody.appendChild(categoryRow);


          section.data.forEach((row) => {
            const tr = document.createElement("tr");
            row.forEach((cell) => {
              const td = document.createElement("td");
              td.textContent = cell;
              tr.appendChild(td);
            });
            tableBody.appendChild(tr);
          });
        });
      }


      // Populate tables
      populateTable("crash-test-spec-table-body", specData);
      populateTable("crash-test-aop-table-body", aopData);
      populateTable("crash-test-passive-safety-table", passiveSafetyData);
      populateStasTable();
      populateTable("crash-test-variant-table-body", variantAvailabilityData);
  </script>
<style>
	      .crash-test-tabs-container {
        display: flex;
        align-items: center;
        position: relative;
      }


      .crash-test-arrow {
        cursor: pointer;
        font-size: 20px;
        font-weight: bold;
        padding: 10px;
        background-color: transparent;
        border: none;
        outline: none;
        display: flex;
        align-items: center;
        justify-content: center;
        width: 30px;
        height: 30px;
        visibility: hidden;
      }


      .crash-test-arrow.crash-test-visible {
        visibility: visible;
      }


      .crash-test-arrow::before {
        content: "";
        display: block;
        width: 10px;
        height: 10px;
        border: solid #ffb400;
        border-width: 0 2px 2px 0;
        transform: rotate(135deg);
      }


      .crash-test-arrow.crash-test-right::before {
        transform: rotate(-45deg);
      }


      .crash-test-tabs {
        display: flex;
        overflow-x: auto;
        white-space: nowrap;
        flex-grow: 1;
        scrollbar-width: none;
        -ms-overflow-style: none;
      }


      .crash-test-tabs::-webkit-scrollbar {
        display: none;
      }


      .crash-test-tab-content {
        display: none;
        margin-top: 10px;
      }


      .crash-test-tab-content.crash-test-active {
        display: block;
      }


      .crash-test-tabs {
        display: flex;
        border-bottom: 1px solid #ddd;
        white-space: nowrap;
        overflow-y: scroll;
        scrollbar-width: none;
        gap: 15px;
      }


      .crash-test-tab {
        padding: 10px 0px;
        cursor: pointer;
        font-weight: bold;
        color: #8c8c8c;
        font-family: "Roboto";
        font-size: 14px;
      }


      .crash-test-tab.crash-test-active {
        color: #262626;
        border-bottom: 3px solid #ffb400;
      }


      .crash-test-tab-content {
        display: none;
        margin-top: 10px;
      }


      .crash-test-tab-content.crash-test-active {
        display: block;
      }


      table {
        width: 100%;
        border-collapse: collapse;
        margin: 20px auto;
      }


      th,
      td {
        padding: 12px;
        border: 1px solid #ddd;
        text-align: center;
        color: #262626;
        font-family: "Roboto";
        font-size: 14px;
      }


      th {
        background-color: #f9f9f9;
        font-weight: bold;
        text-align: center;
      }


      .crash-test-score-column {
        text-align: right;
      }


      .crash-test-footnote {
        font-size: 0.9em;
        color: #666;
        margin-top: 8px;
        margin-bottom: 20px;
      }


      .crash-test-section-header {
        background-color: #fff;
        font-weight: bold;
        text-align: center;
      }


      .crash-test-video-box {
        display: flex;
        justify-content: center;
      }


      .crash-test-cop-image {
        width: 100%;
        height: 100%;
        float: none;
        display: block;
        object-fit: cover;
      }

</style>
      <?php
      return ob_get_clean();
  }
  add_shortcode('crash_test_table_widget', 'crash_test_table_widget');


