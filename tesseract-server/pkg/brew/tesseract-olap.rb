class TesseractOlap < Formula
  version '0.13.0'
  desc "ROLAP engine for web applications, in Rust."
  homepage "https://github.com/hwchen/tesseract"
  url "https://github.com/hwchen/tesseract/releases/download/v#{version}/tesseract-olap-#{version}-x86_64-apple-darwin.tar.gz"
  sha256 "c72b7f5f9c11d93069aac1646f84c8ba3900ac5f19a3de2277ada8543b7d112e"

  def install
    bin.install "tesseract-olap"
  end
end

