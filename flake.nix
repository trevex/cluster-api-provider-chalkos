{
  description = "Cluster API bootstrap and control-plane providers for chalkos";

  # Follows chalkos's own nixpkgs and go-overlay, so the toolchain this devShell builds is the one
  # chalkos builds for itself, and both repositories move together when chalkos's pin is bumped.
  # The input names the chalkos release that go.mod requires, and Renovate bumps both together.
  inputs = {
    chalkos.url = "github:trevex/chalkos/v0.8.0";
    nixpkgs.follows = "chalkos/nixpkgs";
    go-overlay.follows = "chalkos/go-overlay";
  };

  outputs =
    {
      chalkos,
      nixpkgs,
      go-overlay,
      ...
    }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};
      # The Go toolchain go.mod names, the same way chalkos resolves its own.
      go = (go-overlay.lib.mkGoBin pkgs).fromGoModStrict ./go.mod;
    in
    {
      # The images of the KubeVirt lab in lab/: lab-image, lab-image-oci and lab-disk.
      packages.${system} = import ./nix/lab-images.nix { inherit chalkos pkgs; };

      devShells.${system} = rec {
        # The tools of CI's Go jobs, without the lab's, whose chalkbuild builds from source.
        ci = pkgs.mkShell {
          packages = [
            go
            pkgs.kubernetes-controller-tools # controller-gen
            pkgs.kustomize
            pkgs.setup-envtest
            pkgs.golangci-lint
            # govulncheck built with the Go release go.mod names, which also types the standard
            # library it checks.
            go.tools.govulncheck.latest
          ];
        };

        # The tools of CI's workflows job, a shell of their own that keeps Renovate's Node.js out of
        # the Go jobs.
        workflows = pkgs.mkShell {
          packages = [
            pkgs.actionlint
            pkgs.shellcheck
            pkgs.renovate # renovate-config-validator
          ];
        };

        default = pkgs.mkShell {
          inputsFrom = [
            ci
            workflows
          ];
          packages = [
            pkgs.kind
            pkgs.kubectl
            # clusterctl of the Cluster API release go.mod names, built as a go.mod tool, so the
            # lab installs the core release the providers build against. It runs inside the
            # repository.
            (pkgs.writeShellScriptBin "clusterctl" ''exec go tool clusterctl "$@"'')
            pkgs.kubevirt # virtctl
            pkgs.just
            pkgs.crane
            pkgs.qemu # qemu-img
            # Pushes the lab's chalkos image to the lab's registry.
            chalkos.packages.${system}.chalkbuild
          ];
        };
      };
    };
}
